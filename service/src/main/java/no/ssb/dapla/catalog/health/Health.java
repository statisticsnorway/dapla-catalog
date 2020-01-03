package no.ssb.dapla.catalog.health;

import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.webserver.Routing;
import io.helidon.webserver.Service;
import io.helidon.webserver.WebServer;
import io.vertx.pgclient.PgPool;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Health implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(Health.class);

    private final AtomicReference<ReadinessSample> lastReadySample;
    private final int readinessIdleTimeout;
    private final AtomicBoolean pendingReadinessCheck = new AtomicBoolean();
    private final Supplier<WebServer> webServerSupplier;

    public Health(Config config, AtomicReference<ReadinessSample> lastReadySample, Supplier<WebServer> webServerSupplier) {
        this.lastReadySample = lastReadySample;
        this.readinessIdleTimeout = config.get("health.readiness.idle-timeout").asInt().orElse(5000);
        this.webServerSupplier = webServerSupplier;
       }

    @Override
    public void update(Routing.Rules rules) {
        rules.register(HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())
                .addLiveness(() -> HealthCheckResponse.named("LivenessCheck")
                      .up()
                      .withData("time", System.currentTimeMillis())
                      .build())
                .build());
    }
}
