package no.ssb.dapla.catalog;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.tracing.TracerBuilder;
import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.B3TextMapCodec;
import io.jaegertracing.spi.Extractor;
import io.jaegertracing.spi.Injector;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import io.opentracing.contrib.grpc.TracingClientInterceptor.ClientRequestAttribute;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;

public class ApplicationBuilder extends DefaultHelidonApplicationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationBuilder.class);

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
        System.setProperty(Configuration.JAEGER_SERVICE_NAME, "catalog");
        System.setProperty(Configuration.JAEGER_ENDPOINT, "jaeger-collector.istio-system.svc.cluster.local:14268/api/traces");

        Injector<TextMap> b3CodecIn = new B3TextMapCodec();
        Extractor<TextMap> b3CodecEx= new B3TextMapCodec();
        JaegerTracer.Builder tracerBuilder = Configuration.fromEnv().getTracerBuilder()
            .registerInjector(Format.Builtin.HTTP_HEADERS, b3CodecIn)
            .registerExtractor(Format.Builtin.HTTP_HEADERS, b3CodecEx);
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

        return new Application(config, tracer, authService);
    }
}
