package no.ssb.dapla.catalog;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.helidon.config.Config;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;

import static java.util.Optional.ofNullable;

public class ApplicationBuilder extends DefaultHelidonApplicationBuilder {
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
            authGrpcClientChannel = ManagedChannelBuilder
                    .forAddress(
                            config.get("auth-service").get("host").asString().orElseThrow(() ->
                                    new RuntimeException("missing configuration: auth-service.host")),
                            config.get("auth-service").get("port").asInt().orElseThrow(() ->
                                    new RuntimeException("missing configuration: auth-service.port"))
                    )
                    .usePlaintext()
                    .build();
        }

        AuthServiceGrpc.AuthServiceFutureStub authService = AuthServiceGrpc.newFutureStub(authGrpcClientChannel);

        return new Application(config, authService);
    }
}
