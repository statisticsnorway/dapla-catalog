package no.ssb.dapla.catalog;

import ch.qos.logback.classic.util.ContextInitializer;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
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
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.dataset.DatasetRepository;
import no.ssb.dapla.catalog.dataset.DatasetService;
import no.ssb.dapla.catalog.dataset.NameIndex;
import no.ssb.dapla.catalog.dataset.NameService;
import no.ssb.dapla.catalog.dataset.PrefixService;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

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

public class Application implements HelidonApplication{

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

        HelidonApplication application = new ApplicationBuilder().build();

        application.start().toCompletableFuture().orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    Application(Config config, AuthServiceGrpc.AuthServiceFutureStub authService) {
        put(Config.class, config);

        BigtableTableAdminClient bigtableTableAdminClient = BigtableInitializer.createBigtableAdminClient(config.get("bigtable"));
        put(BigtableTableAdminClient.class, bigtableTableAdminClient);

        BigtableInitializer.createBigtableSchemaIfNotExists(config.get("bigtable"), bigtableTableAdminClient);

        BigtableDataClient dataClient = BigtableInitializer.createBigtableDataClient(config.get("bigtable"));
        put(BigtableDataClient.class, dataClient);

        DatasetRepository repository = new DatasetRepository(dataClient);
        put(DatasetRepository.class, repository);

        NameIndex nameIndex = new NameIndex(dataClient);
        put(NameIndex.class, nameIndex);

        // dataset access grpc service
        put(AuthServiceGrpc.AuthServiceFutureStub.class, authService);

        DatasetService dataSetService = new DatasetService(repository, nameIndex, authService);
        put(DatasetService.class, dataSetService);

        NameService nameService = new NameService(nameIndex);
        put(NameService.class, nameService);

        PrefixService prefixService = new PrefixService(nameIndex);
        put(PrefixService.class, prefixService);

        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.create(config.get("grpcserver")),
                GrpcRouting.builder()
                        .register(dataSetService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);

        Routing routing = Routing.builder()
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register("/dataset", dataSetService)
                .register("/name", nameService)
                .register("/prefix", prefixService)
                .build();
        put(Routing.class, routing);

        ServerConfiguration configuration = ServerConfiguration.builder(config.get("webserver")).build();
        WebServer webServer = WebServer.create(configuration, routing);
        put(WebServer.class, webServer);
    }

    public CompletionStage<Application> start() {
        return get(WebServer.class).start()
                .thenCombine(get(GrpcServer.class).start(), (webServer, grpcServer) -> this);
    }

    public CompletionStage<Application> stop() {
        return get(WebServer.class).shutdown()
                .thenCombine(get(GrpcServer.class).shutdown(), ((webServer, grpcServer) -> this))
                .thenCombine(CompletableFuture.runAsync(() -> get(BigtableDataClient.class).close()), (app, v) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> get(BigtableTableAdminClient.class).close()), (app, v) -> this);
    }
}
