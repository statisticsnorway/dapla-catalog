package no.ssb.dapla.catalog;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import io.opentracing.contrib.grpc.TracingClientInterceptor.ClientRequestAttribute;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;

public class CatalogApplicationBuilder extends DefaultHelidonApplicationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogApplicationBuilder.class);

    ManagedChannel authGrpcClientChannel;

    @Override
    public <T> HelidonApplicationBuilder override(Class<T> clazz, T instance) {
        super.override(clazz, instance);
        if (ManagedChannel.class.isAssignableFrom(clazz)) {
            authGrpcClientChannel = (ManagedChannel) instance;
        }
        return this;
    }

    @Override
    public HelidonApplication build() {
        Config config = ofNullable(this.config).orElseGet(() -> createDefaultConfig());

        if (authGrpcClientChannel == null) {
            String host = config.get("auth-service").get("host").asString().orElseThrow(() ->
                    new RuntimeException("missing configuration: auth-service.host"));
            int port = config.get("auth-service").get("port").asInt().orElseThrow(() ->
                    new RuntimeException("missing configuration: auth-service.port"));
            LOG.debug("Creating authGrpcClientChannel with configuration: host={}, port={}", host, port);
            authGrpcClientChannel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();
        }

        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(false);
        Tracer tracer = tracerBuilder.build();

        TracingClientInterceptor tracingInterceptor = TracingClientInterceptor.newBuilder()
                .withTracer(tracer)
                .withStreaming()
                .withVerbosity()
                .withOperationName(new OperationNameConstructor() {
                    @Override
                    public <ReqT, RespT> String constructOperationName(MethodDescriptor<ReqT, RespT> method) {
                        return "Grpc client to " + method.getFullMethodName();
                    }
                })
                .withActiveSpanSource(() -> tracer.scopeManager().activeSpan())
                .withTracedAttributes(ClientRequestAttribute.ALL_CALL_OPTIONS, ClientRequestAttribute.HEADERS)
                .build();

        AuthServiceGrpc.AuthServiceFutureStub authService = AuthServiceGrpc.newFutureStub(tracingInterceptor.intercept(authGrpcClientChannel));

        return new CatalogApplication(config, tracer, authService);
    }
}
