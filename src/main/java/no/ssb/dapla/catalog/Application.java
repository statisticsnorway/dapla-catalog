package no.ssb.dapla.catalog;

import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import io.helidon.grpc.server.GrpcServerConfiguration;
import io.helidon.grpc.server.GrpcTracingConfig;
import io.helidon.grpc.server.ServerRequestAttribute;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.asyncsql.PostgreSQLClient;
import io.vertx.reactivex.ext.sql.SQLClient;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.dataset.CatalogGrpcService;
import no.ssb.dapla.catalog.dataset.DatasetRepository;
import no.ssb.dapla.catalog.dataset.DatasetUpstreamGooglePubSubIntegration;
import no.ssb.dapla.catalog.health.Health;
import no.ssb.dapla.catalog.health.HealthAwareSQLClient;
import no.ssb.dapla.catalog.health.ReadinessSample;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonGrpcWebTranscoding;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import no.ssb.pubsub.EmulatorPubSub;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.RealPubSub;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.ofNullable;

public class Application extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new ApplicationBuilder().build()
                .start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    Application(Config config, Tracer tracer, AuthServiceGrpc.AuthServiceFutureStub authService) {
        put(Config.class, config);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        // reactive postgres client
        Vertx vertx = Vertx.vertx(); // create vertx instance to handle io of sql-client
        SQLClient sqlClient = new HealthAwareSQLClient(initReactiveSqlClient(config.get("pgpool"), vertx), lastReadySample);

        // initialize health, including a database connectivity wait-loop
        Health health = new Health(config, sqlClient, lastReadySample, () -> get(WebServer.class));

        // schema migration using flyway and jdbc
        migrateDatabaseSchema(config.get("flyway"));

        DatasetRepository repository = new DatasetRepository(sqlClient);
        put(DatasetRepository.class, repository);

        if (config.get("pubsub.enabled").asBoolean().orElse(false)) {
            PubSub pubSub = createPubSub(config.get("pubsub"));
            put(PubSub.class, pubSub);

            DatasetUpstreamGooglePubSubIntegration datasetUpstreamSubscriber = new DatasetUpstreamGooglePubSubIntegration(config.get("pubsub.upstream"), pubSub, repository);
            put(DatasetUpstreamGooglePubSubIntegration.class, datasetUpstreamSubscriber);
        }

        // dataset-access grpc client
        put(AuthServiceGrpc.AuthServiceFutureStub.class, authService);

        CatalogGrpcService dataSetGrpcService = new CatalogGrpcService(repository, authService);
        put(CatalogGrpcService.class, dataSetGrpcService);

        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.builder(config.get("grpcserver"))
                        .tracer(tracer)
                        .tracingConfig(GrpcTracingConfig.builder()
                                .withStreaming()
                                .withVerbosity()
                                .withOperationName(new OperationNameConstructor() {
                                    @Override
                                    public <ReqT, RespT> String constructOperationName(MethodDescriptor<ReqT, RespT> method) {
                                        return "Grpc server received " + method.getFullMethodName();
                                    }
                                })
                                .withTracedAttributes(ServerRequestAttribute.CALL_ATTRIBUTES,
                                        ServerRequestAttribute.HEADERS,
                                        ServerRequestAttribute.METHOD_NAME)
                                .build()
                        ),
                GrpcRouting.builder()
                        .intercept(new AuthorizationInterceptor())
                        .register(dataSetGrpcService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register(health)
                .register("/rpc", new HelidonGrpcWebTranscoding(
                        () -> ManagedChannelBuilder
                                .forAddress("localhost", Optional.of(grpcServer)
                                        .filter(GrpcServer::isRunning)
                                        .map(GrpcServer::port)
                                        .orElseThrow())
                                .usePlaintext()
                                .build(),
                        dataSetGrpcService
                ))
                .build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

    private void migrateDatabaseSchema(Config flywayConfig) {
        Flyway flyway = Flyway.configure()
                .dataSource(
                        flywayConfig.get("url").asString().orElse("jdbc:postgresql://localhost:15432/rdc"),
                        flywayConfig.get("user").asString().orElse("rdc"),
                        flywayConfig.get("password").asString().orElse("rdc")
                )
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
                LOG.info("Running with the service-account google bigtable credentials provider");
                String serviceAccountKeyPath = config.get("credentials.service-account.path").asString().orElse(null);
                return RealPubSub.createWithServiceAccountKeyCredentials(serviceAccountKeyPath);
            } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
                LOG.info("Running with the compute-engine google bigtable credentials provider");
                return RealPubSub.createWithComputeEngineCredentials();
            } else { // default
                LOG.info("Running with the default google bigtable credentials provider");
                return RealPubSub.createWithDefaultCredentials();
            }
        }
    }

    private SQLClient initReactiveSqlClient(Config config, Vertx vertx) {
        Config connectConfig = config.get("connect-options");
        JsonObject postgreSQLClientConfig = new JsonObject()
                .put("host", connectConfig.get("host").asString().orElse("localhost"))
                .put("port", connectConfig.get("port").asInt().orElse(15432))
                .put("username", connectConfig.get("user").asString().orElse("rdc"))
                .put("password", connectConfig.get("password").asString().orElse("rdc"))
                .put("database", connectConfig.get("database").asString().orElse("rdc"));
        SQLClient sqlClient = PostgreSQLClient.createShared(vertx, postgreSQLClientConfig);
        return sqlClient;
    }

    @Override
    public CompletionStage<HelidonApplication> stop() {
        return super.stop()
                .thenCombine(CompletableFuture.runAsync(() -> get(SQLClient.class).close()), (a, v) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> ofNullable(get(DatasetUpstreamGooglePubSubIntegration.class)).ifPresent(DatasetUpstreamGooglePubSubIntegration::close)), (a, v) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> ofNullable(get(PubSub.class)).ifPresent(PubSub::close)), (a, v) -> this);
    }
}
