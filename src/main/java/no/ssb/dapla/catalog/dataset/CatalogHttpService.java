package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromHttp;

public class CatalogHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogHttpService.class);

    final DatasetRepository repository;

    public CatalogHttpService(DatasetRepository repository) {
        this.repository = repository;
    }

    private final int limit = 100;

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void update(Routing.Rules rules) {
        LOG.info("rules: {}", rules);
        rules.get("/", this::doGetList);
        rules.get("/{pathPart}", this::doGetList);
        rules.post("/pollute", this::setDirty);

    }

    private void setDirty(ServerRequest req, ServerResponse res) {
        TracerAndSpan tracerAndSpan = spanFromHttp(req, "pollute");
        String path = req.queryParams().first("path").toString();
        Span span = tracerAndSpan.span();
        try {
            repository.setPathDirty(path, Dataset.IsDirty.DIRTY)
                    .timeout(5, TimeUnit.SECONDS)
                    .subscribe(success -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        Tracing.traceOutputMessage(span, path);
                        span.finish();
                        res.send();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in setting path dirty");
                            LOG.error(String.format("path='%s'", path), throwable);
                        } finally {
                            span.finish();
                        }
                    });

        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }

    private void doGetList(ServerRequest req, ServerResponse res) {
        TracerAndSpan tracerAndSpan = spanFromHttp(req, "doGetAll");
        String pathPart = req.path().param("pathPart");
        Span span = tracerAndSpan.span();
        try {
            String finalPathPart = pathPart != null ? pathPart : "";
            repository.listDatasets(finalPathPart, limit)
                    .timeout(5, TimeUnit.SECONDS)
                    .toList()
                    .subscribe(datasets -> {
                        Tracing.restoreTracingContext(tracerAndSpan);

                        ObjectNode jsonCatalogs = objectMapper.createObjectNode();
                        ArrayNode catalogList = jsonCatalogs.putArray("catalogs");
                        for (Dataset dataset : datasets) {
                            catalogList.addObject().putObject("id").put("path", dataset.getId().getPath());;
                        }

                        res.send(jsonCatalogs.toString());
                        span.finish();
                        res.send();
                        Tracing.traceOutputMessage(span, jsonCatalogs.toString());
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in nameIndex.listByPrefix()");
                            LOG.error(String.format("nameIndex.listByPrefix(): prefix='%s'", finalPathPart), throwable);
                        } finally {
                            span.finish();
                        }
                    });
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }


}
