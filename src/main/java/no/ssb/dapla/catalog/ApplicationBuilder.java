package no.ssb.dapla.catalog;

import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import io.grpc.services.internal.HealthCheckingRoundRobinLoadBalancerProvider;
import io.helidon.config.Config;
import io.helidon.config.spi.ConfigSource;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;

public class ApplicationBuilder implements HelidonApplicationBuilder {
    Config config;
    ManagedChannel globalGrpcClientChannel;

    @Override
    public <T> HelidonApplicationBuilder override(Class<T> clazz, T instance) {
        if (Config.class.isAssignableFrom(clazz)) {
            config = (Config) instance;
        }
        if (ManagedChannel.class.isAssignableFrom(clazz)) {
            globalGrpcClientChannel = (ManagedChannel) instance;
        }
        return this;
    }

    @Override
    public HelidonApplication build() {
        applyGrpcProvidersWorkaround();

        if (config == null) {
            List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList<>();
            String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
            if (overrideFile != null) {
                configSourceSupplierList.add(file(overrideFile).optional());
            }
            configSourceSupplierList.add(file("conf/application.yaml").optional());
            configSourceSupplierList.add(classpath("application.yaml"));

            config = Config.builder().sources(configSourceSupplierList).build();
        }

        CatalogServiceGrpc.CatalogServiceFutureStub catalogService;
        AuthServiceGrpc.AuthServiceFutureStub authService;
        if (globalGrpcClientChannel == null) {
            ManagedChannel datasetAccessChannel = ManagedChannelBuilder
                    .forAddress(
                            config.get("auth-service").get("host").asString().orElse("localhost"),
                            config.get("auth-service").get("port").asInt().orElse(7070)
                    )
                    .usePlaintext()
                    .build();
            authService = AuthServiceGrpc.newFutureStub(datasetAccessChannel);
        } else {
            authService = AuthServiceGrpc.newFutureStub(globalGrpcClientChannel);
        }

        Application application = new Application(config, authService);
        return application;
    }

    private void applyGrpcProvidersWorkaround() {
        // The shaded version of grpc from helidon does not include the service definition for
        // PickFirstLoadBalancerProvider. This result in LoadBalancerRegistry not being able to
        // find it. We register them manually here.
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        LoadBalancerRegistry.getDefaultRegistry().register(new HealthCheckingRoundRobinLoadBalancerProvider());

        // The same thing happens with the name resolvers.
        NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());
    }
}
