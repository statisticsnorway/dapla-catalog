package no.ssb.dapla.catalog;

import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplicationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogApplicationBuilder extends DefaultHelidonApplicationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogApplicationBuilder.class);

    private UserAccessClient userAccessClient;

    @Override
    public HelidonApplicationBuilder override(Class<?> clazz, Object instance) {
        if (UserAccessClient.class.isAssignableFrom(clazz)) {
            this.userAccessClient = (UserAccessClient) instance;
        }
        return super.override(clazz, instance);
    }

    @Override
    public CatalogApplication build() {
        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(false);
        Tracer tracer = tracerBuilder.build();

        if (userAccessClient == null) {
            String userAccessHost = config.get("auth-service").get("host").asString().orElseThrow(() ->
                    new RuntimeException("missing configuration: auth-service.host"));
            int userAccessPort = config.get("auth-service").get("port").asInt().orElseThrow(() ->
                    new RuntimeException("missing configuration: auth-service.port"));
            userAccessClient = new UserAccessWebClient(userAccessHost, userAccessPort);
        }

        return new CatalogApplication(config, tracer, userAccessClient);
    }
}
