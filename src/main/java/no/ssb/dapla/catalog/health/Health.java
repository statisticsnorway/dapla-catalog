package no.ssb.dapla.catalog.health;

import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.webserver.Routing;
import io.helidon.webserver.Service;
import io.helidon.webserver.WebServer;
import io.vertx.reactivex.ext.sql.SQLClient;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Health implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(Health.class);

    private final SQLClient sqlClient;
    private final AtomicReference<ReadinessSample> lastReadySample;
    private final int readinessIdleTimeout;
    private final AtomicBoolean pendingReadinessCheck = new AtomicBoolean();
    private final Supplier<WebServer> webServerSupplier;

    public Health(Config config, SQLClient sqlClient, AtomicReference<ReadinessSample> lastReadySample, Supplier<WebServer> webServerSupplier) {
        this.sqlClient = sqlClient;
        this.lastReadySample = lastReadySample;
        this.readinessIdleTimeout = config.get("health.readiness.idle-timeout").asInt().orElse(5000);
        this.webServerSupplier = webServerSupplier;
        blockingCheckDatabaseConnectivity(config, sqlClient);
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.register(HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())
                .addLiveness(() -> HealthCheckResponse.named("LivenessCheck")
                        .up()
                        .withData("time", System.currentTimeMillis())
                        .build())
                .addReadiness(() -> {
                    ReadinessSample sample = getAndKeepaliveReadinessSample(sqlClient);
                    return HealthCheckResponse.named("ReadinessCheck")
                            .state(webServerSupplier.get().isRunning() && sample.dbConnected)
                            .withData("time", sample.time)
                            .build();
                })
                .build());
    }

    ReadinessSample getAndKeepaliveReadinessSample(SQLClient sqlClient) {
        ReadinessSample sample = lastReadySample.get();
        if (System.currentTimeMillis() - sample.time > readinessIdleTimeout) {
            if (pendingReadinessCheck.compareAndSet(false, true)) {
                // asynchronously update readiness, the updated value will not be used with the current readiness check,
                // but with the first readiness check called after the lastReadySample is updated.
                executeSelectOne(sqlClient).thenAccept(succeeded -> {
                    lastReadySample.set(new ReadinessSample(succeeded, System.currentTimeMillis()));
                    pendingReadinessCheck.set(false);
                }).exceptionally(throwable -> {
                    lastReadySample.set(new ReadinessSample(false, System.currentTimeMillis()));
                    pendingReadinessCheck.set(false);
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    throw new RuntimeException(throwable);
                });
            }
        }
        return sample;
    }

    void blockingCheckDatabaseConnectivity(Config config, SQLClient sqlClient) {
        // run during initialization, so it's ok to block
        String host = config.get("pgpool.connect-options.host").asString().orElse("unable-to-resolve");
        int port = config.get("pgpool.connect-options.port").asInt().orElse(0);
        boolean dbConnected = false;
        try {
            int attempts = config.get("init.db-connectivity-attempts").asInt().orElse(10);
            for (int i = 1; i <= attempts; i++) {
                try {
                    if (executeSelectOne(sqlClient).get(1, TimeUnit.SECONDS)) {
                        dbConnected = true;
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // preserve
                    throw new RuntimeException(e);
                } catch (ExecutionException | TimeoutException e) {
                    LOG.debug("", e);
                }
                LOG.debug("Unable to connect to {}:{}. Attempt: {}", host, port, i);
                if (i + 1 <= attempts) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // preserve
                        throw new RuntimeException(e);
                    }
                }
            }
        } finally {
            lastReadySample.set(new ReadinessSample(dbConnected, System.currentTimeMillis()));
        }
        String database = config.get("pgpool.connect-options.database").asString().orElse("unable-to-resolve");
        String user = config.get("pgpool.connect-options.user").asString().orElse("unable-to-resolve");
        if (dbConnected) {
            LOG.info("Successfully connected to {}:{}/{} with user {} and password *****", host, port, database, user);
        } else {
            LOG.error("Unable to connect to {}:{}/{} with user {} and password *****", host, port, database, user);
            throw new RuntimeException("Unable to initialize application, unable to establish database connectivity");
        }
    }

    CompletableFuture<Boolean> executeSelectOne(SQLClient sqlClient) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        sqlClient.query("SELECT 1", ar -> {
            if (ar.succeeded()) {
                future.complete(true);
            } else {
                future.complete(false);
            }
        });
        return future;
    }
}
