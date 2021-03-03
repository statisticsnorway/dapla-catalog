package no.ssb.dapla.catalog.dataset;

import io.helidon.metrics.RegistryFactory;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.CreateTableRequest;
import no.ssb.dapla.catalog.protobuf.GetTableRequest;
import no.ssb.dapla.catalog.protobuf.GetTableResponse;
import no.ssb.dapla.catalog.protobuf.UpdateTableRequest;
import no.ssb.helidon.application.Tracing;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.*;

public class TableHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(TableHttpService.class);

    final TableRepository repository;
    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Counter getRequestCounter;
    private final Counter getFailedCounter;
    private final Counter createRequestCounter;
    private final Counter createFailedCounter;
    private final Counter updateRequestCounter;
    private final Counter updateFailedCounter;

    public TableHttpService(TableRepository repository) {
        this.repository = repository;
        RegistryFactory metricsRegistry = RegistryFactory.getInstance();
        MetricRegistry appRegistry = metricsRegistry.getRegistry(MetricRegistry.Type.APPLICATION);
        this.getRequestCounter = appRegistry.counter("getRequestCount");
        this.getFailedCounter = appRegistry.counter("getFailedCount");
        this.createRequestCounter = appRegistry.counter("createRequestCount");
        this.createFailedCounter = appRegistry.counter("createFailedCount");
        this.updateRequestCounter = appRegistry.counter("updateRequestCount");
        this.updateFailedCounter = appRegistry.counter("updateFailedCount");
    }

    @Override
    public void update(Routing.Rules rules) {
        LOG.info("rules: {}", rules);
        rules.post("/catalog2/get", Handler.create(GetTableRequest.class, this::getTable));
        rules.post("/catalog2/create", Handler.create(CreateTableRequest.class, this::createTable));
        rules.put("/catalog2/update", Handler.create(UpdateTableRequest.class, this::updateTable));
    }

    public void getTable(ServerRequest req, ServerResponse res, GetTableRequest getTableRequest) {
        getRequestCounter.inc();
        Span span = spanFromHttp(req, "getTable");
        try {
            repository.get(getTableRequest.getPath())
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .toOptionalSingle()
                    .subscribe(tableOpt -> {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        if (tableOpt.isPresent()) {
                            GetTableResponse.Builder builder = GetTableResponse.newBuilder()
                                    .setTable(tableOpt.get());
                            res.status(200).send(builder.build());
                        } else {
                            res.status(404).send();
                        }
                        span.finish();
                    }, throwable -> {
                        try {
                            getFailedCounter.inc();
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error in repository.get()");
                            LOG.error(String.format("repository.get(): path='%s'", getTableRequest.getPath()), throwable);
                            res.status(500).send(throwable.getMessage());
                        } finally {
                            span.finish();
                        }
                    });
        } catch (RuntimeException | Error e) {
            try {
                getFailedCounter.inc();
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }

    public void createTable(ServerRequest req, ServerResponse res, CreateTableRequest createTableRequest) {
        createRequestCounter.inc();
        Span span = spanFromHttp(req, "createTable");
        try {
            repository.create(createTableRequest.getTable())
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .toOptionalSingle()
                    .subscribe(createdCount -> {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        res.send();
                        span.finish();
                    }, throwable -> {
                        try {
                            createFailedCounter.inc();
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error in repository.create()");
                            LOG.error(String.format("repository.create(): path='%s'", createTableRequest.getTable().getPath()), throwable);
                            res.status(500).send(throwable.getMessage());
                        } finally {
                            span.finish();
                        }
                    });
        } catch (RuntimeException | Error e) {
            try {
                createFailedCounter.inc();
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }

    public void updateTable(ServerRequest req, ServerResponse res, UpdateTableRequest updateTableRequest) {
        updateRequestCounter.inc();
        Span span = spanFromHttp(req, "createTable");
        try {
            repository.update(updateTableRequest.getTable(), updateTableRequest.getPreviousMetadataLocation())
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .toOptionalSingle()
                    .subscribe(updatedCount -> {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        if (updatedCount.isEmpty() || !updatedCount.get().equals(1L)) {
                            String errorStr = String.format("Failed to update table with path '%s' and metadata location '%s'. " +
                                            "Maybe another process changed it.", updateTableRequest.getTable().getPath(),
                                    updateTableRequest.getPreviousMetadataLocation());
                            LOG.warn(errorStr);
                            updateFailedCounter.inc();
                            res.status(400).send(errorStr);
                        } else {
                            res.send();
                            span.finish();
                        }
                    }, throwable -> {
                        try {
                            updateFailedCounter.inc();
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error in repository.update()");
                            LOG.error(String.format("repository.update(): path='%s'", updateTableRequest.getTable().getPath()), throwable);
                            res.status(500).send(throwable.getMessage());
                        } finally {
                            span.finish();
                        }
                    });
        } catch (RuntimeException | Error e) {
            try {
                updateFailedCounter.inc();
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }


}
