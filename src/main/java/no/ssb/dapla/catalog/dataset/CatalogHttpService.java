package no.ssb.dapla.catalog.dataset;


import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.*;

public class CatalogHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogHttpService.class);

    final DatasetRepository repository;

    public CatalogHttpService(DatasetRepository repository) {
        this.repository = repository;
    }

    private final int limit = 100;

    @Override
    public void update(Routing.Rules rules) {
        LOG.info("rules: ", rules);
        rules.get("/", this::doGetAll);
        rules.get("/{prefix}", this::doGetAll);
    }

    private void doGetAll(ServerRequest req, ServerResponse res) {
        LOG.info("doGetAll: ");
        TracerAndSpan tracerAndSpan = spanFromHttp(req, "doGetAll");
        String prefix = req.path().param("prefix");
        LOG.info("doGetAll, prefix: {}", prefix);
        Span span = tracerAndSpan.span();
        try {
            String finalPrefix = prefix != null ? prefix : "";
            repository.listCatalogs(prefix, limit)
                    .timeout(5, TimeUnit.SECONDS)
                    .toList()
                    .subscribe(catalogs -> {
                        LOG.info("catalogs: {}", catalogs);
                        Tracing.restoreTracingContext(tracerAndSpan);

                        StringBuffer jsonCatalogs = new StringBuffer("{ \"catalogs\": [");
                            for (Dataset catalog : catalogs) {
                                LOG.info("catalog: {}", catalog);
                                jsonCatalogs.append(ProtobufJsonUtils.toString(catalog)).append(',');                            }
                            jsonCatalogs.deleteCharAt(jsonCatalogs.length() - 1);
                            jsonCatalogs.append("]}");
                            res.send(jsonCatalogs);
                        span.finish();
                        res.send();
                        Tracing.traceOutputMessage(span, jsonCatalogs.toString());
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in nameIndex.listByPrefix()");
                            LOG.error(String.format("nameIndex.listByPrefix(): prefix='%s'", finalPrefix), throwable);
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
