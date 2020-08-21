package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.helidon.webserver.Handler;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Privilege;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromHttp;

public class CatalogHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogHttpService.class);
    final AuthServiceGrpc.AuthServiceFutureStub authService;
    final CatalogSignatureVerifier verifier;
    final DatasetRepository repository;

    public CatalogHttpService(DatasetRepository repository, CatalogSignatureVerifier verifier, AuthServiceGrpc.AuthServiceFutureStub authService) {
        this.repository = repository;
        this.authService = authService;
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

            boolean verified = verifier.verify(datasetMetaBytes, signatureBytes);
            if (verified) {
                AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                        .setUserId(userId)
                        .setPath(dataset.getId().getPath())
                        .setPrivilege(Privilege.CREATE.name())
                        .setValuation(dataset.getValuation().name())
                        .setState(dataset.getState().name())
                        .build();
                ListenableFuture<AccessCheckResponse> hasAccessListenableFuture = authService
                        .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(AuthorizationInterceptor.token()))
                        .hasAccess(checkRequest);

                Futures.addCallback(hasAccessListenableFuture, new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable AccessCheckResponse result) {

                        Tracing.restoreTracingContext(tracerAndSpan);

                        if (result != null && result.getAllowed()) {
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
                            res.status(403).send(String.format("user $s is forbidden to write to the dataset.", userId));
                            span.finish();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, t, "error in authService.hasAccess()");
                            LOG.error("error in authService.hasAccess()", t);

                            res.status(401).send("error in authService.hasAccess()");
                        } finally {
                            span.finish();
                        }
                    }
                }, MoreExecutors.directExecutor());
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
