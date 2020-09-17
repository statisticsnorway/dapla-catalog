package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.helidon.webserver.Handler;
import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
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
    final CatalogSignatureVerifier verifier;
    final DatasetRepository repository;

    public CatalogHttpService(DatasetRepository repository, CatalogSignatureVerifier verifier) {
        this.repository = repository;
        this.verifier = verifier;
    }

    private final int limit = 100;

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void update(Routing.Rules rules) {
        LOG.info("rules: {}", rules);
        rules.get("/", this::doGetList);
        rules.get("/{pathPart}", this::doGetList);
        rules.post("/write", Handler.create(SignedDataset.class, this::writeDataset));
    }


    private void writeDataset(ServerRequest req, ServerResponse res, SignedDataset signedDataset) {
        TracerAndSpan tracerAndSpan = spanFromHttp(req, "writeDataset");
        Span span = tracerAndSpan.span();
        try {
            String userId = signedDataset.getUserId();
            Dataset dataset = signedDataset.getDataset();
            byte[] signatureBytes = signedDataset.getDatasetMetaSignatureBytes().toByteArray();
            byte[] datasetMetaBytes = signedDataset.getDatasetMetaBytes().toByteArray();

            if (signatureBytes.length == 0 || datasetMetaBytes.length == 0) {
                res.status(400).send("Missing dataset metadata or dataset metadata signature");
            }
            boolean verified = verifier.verify(datasetMetaBytes, signatureBytes);
            if (verified) {
                repository.create(dataset)
                        .timeout(5, TimeUnit.SECONDS)
                        .subscribe(updatedCount -> {
                            Tracing.restoreTracingContext(tracerAndSpan);

                            res.send();
                            span.finish();
                        }, throwable -> {
                            try {
                                Tracing.restoreTracingContext(tracerAndSpan);
                                logError(span, throwable, "error in repository.create()");
                                LOG.error(String.format("repository.create(): dataset-id='%s'", dataset.getId().getPath()), throwable);
                                res.status(505).send();
                            } finally {
                                span.finish();
                            }
                        });

            } else {
                res.status(401).send("Signature Unauthorized.");
            }

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
                        ObjectNode currentDataset;

                        for (Dataset dataset : datasets) {
                            currentDataset = catalogList.addObject();
                            currentDataset.putObject("id")
                                .put("path", dataset.getId().getPath())
                                .put("timestamp", dataset.getId().getTimestamp());
                            currentDataset.put("type", dataset.getType().toString());
                            currentDataset.put("valuation", dataset.getValuation().toString());
                            currentDataset.put("state", dataset.getState().toString());
                            currentDataset.put("pseudoConfig", dataset.getPseudoConfig().getVarsList().toString());
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
