package no.ssb.dapla.catalog;

import ch.qos.logback.classic.util.ContextInitializer;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import io.helidon.dbclient.DbClient;
import io.helidon.dbclient.health.DbClientHealthCheck;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import no.ssb.dapla.catalog.dataset.CatalogHttpService;
import no.ssb.dapla.catalog.dataset.CatalogSignatureVerifier;
import no.ssb.dapla.catalog.dataset.DatasetRepository;
import no.ssb.dapla.catalog.dataset.DatasetUpstreamGooglePubSubIntegration;
import no.ssb.dapla.catalog.dataset.DatasetUpstreamGooglePubSubIntegrationInitializer;
import no.ssb.dapla.catalog.dataset.DefaultCatalogSignatureVerifier;
import no.ssb.dapla.catalog.dataset.TableHttpService;
import no.ssb.dapla.catalog.dataset.TableRepository;
import no.ssb.dapla.catalog.health.ReadinessSample;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import no.ssb.pubsub.EmulatorPubSub;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.RealPubSub;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;

import static java.util.Optional.ofNullable;

public class CatalogApplication extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(CatalogApplication.class);
    }

    public static void installSlf4jJulBridge() {
        // placeholder used to trigger static initializer only
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new CatalogApplicationBuilder().build()
                .start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }

    CatalogApplication(Config config, Tracer tracer, UserAccessClient userAccessClient) {
        put(Config.class, config);

        put(UserAccessClient.class, userAccessClient);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        // schema migration using flyway and jdbc
        migrateDatabaseSchema(config.get("flyway"));

        DbClient dbClient = DbClient.builder()
                .config(config.get("db"))
                .build();

        HealthSupport health = HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())
                .addLiveness(DbClientHealthCheck.builder(dbClient).query().build())
                .addReadiness()
                .build();

        // initialize health, including a database connectivity wait-loop
        // Health health = new Health(config, sqlClient, lastReadySample, () -> get(WebServer.class));

        DatasetRepository repository = new DatasetRepository(dbClient);
        TableRepository tableRepository = new TableRepository(dbClient);

        put(DatasetRepository.class, repository);
        put(TableRepository.class, tableRepository);


        CatalogSignatureVerifier catalogSignatureVerifier;
        Config signerConfig = config.get("catalogds");
        if (signerConfig.get("bypass-validation").asBoolean().orElse(false)) {
            catalogSignatureVerifier = (data, receivedSign) -> true; // always accept signature
        } else {
            String keystoreFormat = signerConfig.get("format").asString().get();
            String keystore = signerConfig.get("keystore").asString().get();
            String keyAlias = signerConfig.get("keyAlias").asString().get();
            char[] password = signerConfig.get("password-file").asString()
                    .filter(s -> !s.isBlank())
                    .map(passwordFile -> Path.of(passwordFile))
                    .filter(Files::exists)
                    .map(CatalogApplication::readPasswordFromFile)
                    .orElseGet(() -> signerConfig.get("password").asString().get().toCharArray());
            String algorithm = signerConfig.get("algorithm").asString().get();
            catalogSignatureVerifier = new DefaultCatalogSignatureVerifier(keystoreFormat, keystore, keyAlias, password, algorithm);
        }
        put(CatalogSignatureVerifier.class, catalogSignatureVerifier);

        if (config.get("pubsub.enabled").asBoolean().orElse(false)) {
            LOG.info("Running with PubSub enabled");

            PubSub pubSub = createPubSub(config.get("pubsub"));
            put(PubSub.class, pubSub);

            LOG.info("Created PubSub of class type: " + pubSub.getClass().getName());

            if (config.get("pubsub.admin").asBoolean().orElse(false)) {
                LOG.info("Admin of topics and subscriptions enabled, running initializer");
                DatasetUpstreamGooglePubSubIntegrationInitializer.initializeTopicsAndSubscriptions(config.get("pubsub.upstream"), pubSub);
            }

            DatasetUpstreamGooglePubSubIntegration datasetUpstreamSubscriber = new DatasetUpstreamGooglePubSubIntegration(config.get("pubsub.upstream"), pubSub, repository);
            put(DatasetUpstreamGooglePubSubIntegration.class, datasetUpstreamSubscriber);

            LOG.info("Subscribed upstream");
        }

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(MetricsSupport.create())
                .register(health)
                .register(new CatalogHttpService(repository, catalogSignatureVerifier, userAccessClient))
                .register(new TableHttpService(tableRepository))
                .build();

        put(Routing.class, routing);

        WebServer webServer = WebServer.builder()
                .config(config.get("webserver"))
                .tracer(tracer)
                .addMediaSupport(ProtobufJsonSupport.create())
                .routing(routing)
                .build();
        put(WebServer.class, webServer);
    }

    private void migrateDatabaseSchema(Config flywayConfig) {
        Flyway flyway = Flyway.configure()
                .dataSource(
                        flywayConfig.get("url").asString().orElse("jdbc:postgresql://localhost:15432/rdc"),
                        flywayConfig.get("user").asString().orElse("rdc"),
                        flywayConfig.get("password").asString().orElse("rdc")
                )
                .connectRetries(flywayConfig.get("connect-retries").asInt().orElse(120))
                .load();
        flyway.migrate();
    }

    static PubSub createPubSub(Config config) {
        boolean useEmulator = config.get("use-emulator").asBoolean().orElse(false);
        if (useEmulator) {
            Config emulatorConfig = config.get("emulator");
            String host = emulatorConfig.get("host").asString().get();
            int port = emulatorConfig.get("port").asInt().get();
            return new EmulatorPubSub(host, port);
        } else {
            String configuredProviderChoice = config.get("credential-provider").asString().orElse("default");
            if ("service-account".equalsIgnoreCase(configuredProviderChoice)) {
                LOG.info("PubSub running with the service-account google credentials provider");
                String serviceAccountKeyPath = config.get("credentials.service-account.path").asString().orElse(null);
                return RealPubSub.createWithServiceAccountKeyCredentials(serviceAccountKeyPath);
            } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
                LOG.info("PubSub running with the compute-engine google credentials provider");
                return RealPubSub.createWithComputeEngineCredentials();
            } else { // default
                LOG.info("PubSub running with the default google credentials provider");
                return RealPubSub.createWithDefaultCredentials();
            }
        }
    }

    private static char[] readPasswordFromFile(Path passwordPath) {
        try {
            return Files.readString(passwordPath).toCharArray();
        } catch (IOException e) {
            return null;
        }
    }

    public Single<CatalogApplication> start() {
        return ofNullable(get(WebServer.class))
                .map(webServer -> webServer.start().map(ws -> this))
                .orElse(Single.just(this));
    }

    public Single<CatalogApplication> stop() {
        return Single.create(ofNullable(get(WebServer.class))
                .map(WebServer::shutdown)
                .orElse(Single.empty())
                .thenCombine(CompletableFuture.runAsync(() -> ofNullable(get(DatasetUpstreamGooglePubSubIntegration.class)).ifPresent(DatasetUpstreamGooglePubSubIntegration::close)), (a, v) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> ofNullable(get(PubSub.class)).ifPresent(PubSub::close)), (a, v) -> this)
        );
    }
}
