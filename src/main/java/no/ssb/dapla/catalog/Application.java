package no.ssb.dapla.catalog;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
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
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.dataset.AuthorizationInterceptor;
import no.ssb.dapla.catalog.dataset.CatalogGrpcService;
import no.ssb.dapla.catalog.dataset.DatasetHttpService;
import no.ssb.dapla.catalog.dataset.DatasetRepository;
import no.ssb.dapla.catalog.dataset.LoggingInterceptor;
import no.ssb.dapla.catalog.dataset.NameHttpService;
import no.ssb.dapla.catalog.dataset.NameIndex;
import no.ssb.dapla.catalog.dataset.PrefixHttpService;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

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

        BigtableTableAdminClient bigtableTableAdminClient = BigtableInitializer.createBigtableAdminClient(config.get("bigtable"));
        put(BigtableTableAdminClient.class, bigtableTableAdminClient);

        BigtableInitializer.createBigtableSchemaIfNotExists(config.get("bigtable"), bigtableTableAdminClient);

        BigtableDataClient dataClient = BigtableInitializer.createBigtableDataClient(config.get("bigtable"));
        put(BigtableDataClient.class, dataClient);

        DatasetRepository repository = new DatasetRepository(dataClient);
        put(DatasetRepository.class, repository);

        NameIndex nameIndex = new NameIndex(dataClient);
        put(NameIndex.class, nameIndex);

        // dataset-access grpc client
        put(AuthServiceGrpc.AuthServiceFutureStub.class, authService);

        CatalogGrpcService dataSetGrpcService = new CatalogGrpcService(repository, nameIndex, authService);
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
                        .intercept(new LoggingInterceptor())
                        .intercept(new AuthorizationInterceptor())
                        .register(dataSetGrpcService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);

        DatasetHttpService dataSetHttpService = new DatasetHttpService(repository, nameIndex, authService);
        put(DatasetHttpService.class, dataSetHttpService);

        NameHttpService nameHttpService = new NameHttpService(nameIndex);
        put(NameHttpService.class, nameHttpService);

        PrefixHttpService prefixHttpService = new PrefixHttpService(nameIndex);
        put(PrefixHttpService.class, prefixHttpService);

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register("/dataset", dataSetHttpService)
                .register("/name", nameHttpService)
                .register("/prefix", prefixHttpService)
                .build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

    @Override
    public CompletionStage<HelidonApplication> stop() {
        return super.stop()
                .thenCombine(CompletableFuture.runAsync(() -> get(BigtableDataClient.class).close()), (app, v) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> get(BigtableTableAdminClient.class).close()), (app, v) -> this);
    }
}
