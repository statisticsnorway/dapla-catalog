package no.ssb.dapla.catalog;

import ch.qos.logback.classic.util.ContextInitializer;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import io.grpc.services.internal.HealthCheckingRoundRobinLoadBalancerProvider;
import io.helidon.config.Config;
import io.helidon.config.spi.ConfigSource;
import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import io.helidon.grpc.server.GrpcServerConfiguration;
import io.helidon.media.jackson.server.JacksonSupport;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import no.ssb.dapla.catalog.dataset.DatasetRepository;
import no.ssb.dapla.catalog.dataset.DatasetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.LogManager;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;

public class Application {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList<>();
        String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
        if (overrideFile != null) {
            configSourceSupplierList.add(file(overrideFile).optional());
        }
        configSourceSupplierList.add(file("conf/application.yaml").optional());
        configSourceSupplierList.add(classpath("application.yaml"));

        Application application = new Application(Config.builder().sources(configSourceSupplierList).build());
        application.start().toCompletableFuture().orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    public Application(Config config) {
        put(Config.class, config);

        // The shaded version of grpc from helidon does not include the service definition for
        // PickFirstLoadBalancerProvider. This result in LoadBalancerRegistry not being able to
        // find it. We register them manually here.
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        LoadBalancerRegistry.getDefaultRegistry().register(new HealthCheckingRoundRobinLoadBalancerProvider());

        // The same thing happens with the name resolvers.
        NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());

        BigtableTableAdminClient adminClient = createBigtableAdminClient(config.get("bigtable"));
        put(BigtableTableAdminClient.class, adminClient);
        createBigtableSchemaIfNotExists(config.get("bigtable"), adminClient);

        BigtableDataClient dataClient = createBigtableDataClient(config.get("bigtable"));
        put(BigtableDataClient.class, dataClient);

        DatasetRepository repository = new DatasetRepository(dataClient);
        put(DatasetRepository.class, repository);

        DatasetService dataSetService = new DatasetService(repository);
        put(DatasetService.class, dataSetService);

        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.create(config.get("grpcserver")),
                GrpcRouting.builder()
                        .register(dataSetService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);

        Routing routing = Routing.builder()
                .register(JacksonSupport.create())
                .register(MetricsSupport.create())
                .register("/dataset", dataSetService)
                .build();
        put(Routing.class, routing);

        ServerConfiguration configuration = ServerConfiguration.builder(config.get("webserver")).build();
        WebServer webServer = WebServer.create(configuration, routing);
        put(WebServer.class, webServer);
    }

    private static void createBigtableSchemaIfNotExists(Config bigtableConfig, BigtableTableAdminClient adminClient) {
        String tableId = bigtableConfig.get("table-id").asString().orElse("dataset");
        if (!adminClient.exists(tableId)) {
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(bigtableConfig.get("column-family").asString().orElse("document"));
            adminClient.createTable(createTableRequest);
        }
    }

    private static BigtableTableAdminClient createBigtableAdminClient(Config bigtableConfig) {
        try {
            BigtableTableAdminSettings settings;
            if (bigtableConfig.get("emulator").asBoolean().orElse(true)) {
                settings = BigtableTableAdminSettings
                        .newBuilderForEmulator(bigtableConfig.get("host").asString().orElse("localhost"), bigtableConfig.get("port").asInt().orElse(9035))
                        .setProjectId(bigtableConfig.get("project-id").asString().orElse("my-project"))
                        .setInstanceId(bigtableConfig.get("instance-id").asString().orElse("my-instance"))
                        .build();
            } else {
                settings = BigtableTableAdminSettings
                        .newBuilder()
                        .setProjectId(bigtableConfig.get("project-id").asString().orElse("my-project"))
                        .setInstanceId(bigtableConfig.get("instance-id").asString().orElse("my-instance"))
//                        .setCredentialsProvider(GoogleCredentialsProvider.newBuilder().build()) //TODO
                        .build();
            }
            return BigtableTableAdminClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Failed to connect to bigtable", e);
        }
    }

    private static BigtableDataClient createBigtableDataClient(Config bigtableConfig) {
        BigtableDataSettings settings;
        if (bigtableConfig.get("emulator").asBoolean().orElse(true)) {
            settings = BigtableDataSettings
                    .newBuilderForEmulator(bigtableConfig.get("host").asString().orElse("localhost"), bigtableConfig.get("port").asInt().orElse(9035))
                    .setProjectId(bigtableConfig.get("project-id").asString().orElse("my-project"))
                    .setInstanceId(bigtableConfig.get("instance-id").asString().orElse("my-instance"))
                    .build();
        } else {
            settings = BigtableDataSettings
                    .newBuilder()
                    .setProjectId(bigtableConfig.get("project-id").asString().orElse("my-project"))
                    .setInstanceId(bigtableConfig.get("instance-id").asString().orElse("my-instance"))
//                        .setCredentialsProvider(GoogleCredentialsProvider.newBuilder().build()) //TODO
                    .build();
        }
        try {
            return BigtableDataClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Could not connect to bigtable", e);
        }
    }

    static final class ApplicationInitializationException extends RuntimeException {
        public ApplicationInitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public CompletionStage<Application> start() {
        return get(WebServer.class).start()
                .thenCombine(get(GrpcServer.class).start(), (webServer, grpcServer) -> this);
    }

    public Application stop() {
        get(WebServer.class).shutdown()
                .thenCombine(get(GrpcServer.class).shutdown(), ((webServer, grpcServer) -> this))
                .thenCombine(CompletableFuture.runAsync(() -> get(BigtableDataClient.class).close()), (app, v) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> get(BigtableTableAdminClient.class).close()), (app, v) -> this)
                .toCompletableFuture().orTimeout(2, TimeUnit.SECONDS).join();
        return this;
    }
}
