package no.ssb.dapla.catalog.dataset;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import io.helidon.common.reactive.Single;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.Privilege;
import no.ssb.dapla.catalog.UserAccessClient;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
import no.ssb.dapla.catalog.protobuf.VarPseudoConfigItem;
import no.ssb.dapla.dataset.api.DatasetMetaAll;
import no.ssb.helidon.application.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromHttp;

public class CatalogHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogHttpService.class);
    final CatalogSignatureVerifier verifier;
    final DatasetRepository repository;
    final UserAccessClient userAccessClient;
    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public CatalogHttpService(DatasetRepository repository, CatalogSignatureVerifier verifier, UserAccessClient userAccessClient) {
        this.repository = repository;
        this.verifier = verifier;
        this.userAccessClient = userAccessClient;
    }

    private final int limit = 100;

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void update(Routing.Rules rules) {
        LOG.info("rules: {}", rules);
        rules.post("/rpc/CatalogService/listByPrefix", Handler.create(ListByPrefixRequest.class, this::listByPrefix));
        rules.post("/rpc/CatalogService/get", Handler.create(GetDatasetRequest.class, this::getDataset));
        rules.post("/rpc/CatalogService/delete", Handler.create(DeleteDatasetRequest.class, this::delete));
        rules.get("/catalog", this::doGetList);
        rules.get("/catalog/{pathPart}", this::doGetList);
        rules.post("/catalog/write", Handler.create(SignedDataset.class, this::writeDataset)); // TODO Use PUT method here!
    }

    private Single<Dataset> repositoryGet(String path, long timestamp) {
        if (path == null) {
            return Single.empty();
        }
        if (timestamp > 0) {
            return repository.get(path, timestamp);
        }
        return repository.get(path);
    }

    public void listByPrefix(ServerRequest req, ServerResponse res, ListByPrefixRequest request) {
        Span span = spanFromHttp(req, "listByPrefix");
        try {
            repository.listByPrefix(request.getPrefix(), 1000)
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .collectList()
                    .subscribe(entries -> {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        ListByPrefixResponse response = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                        res.status(200).send(response);
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error in nameIndex.listByPrefix()");
                            LOG.error(String.format("nameIndex.listByPrefix(): prefix='%s'", request.getPrefix()), throwable);
                            res.status(500).send(throwable.getMessage());
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

    public void getDataset(ServerRequest req, ServerResponse res, GetDatasetRequest getDatasetRequest) {
        Span span = spanFromHttp(req, "getDataset");
        try {
            repositoryGet(getDatasetRequest.getPath(), getDatasetRequest.getTimestamp())
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .toOptionalSingle()
                    .subscribe(datasetOpt -> {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        if (datasetOpt.isPresent()) {
                            GetDatasetResponse.Builder builder = GetDatasetResponse.newBuilder()
                                    .setDataset(datasetOpt.get());
                            res.status(200).send(builder.build());
                        } else {
                            res.status(200).send(GetDatasetResponse.newBuilder().build());
                        }
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error in repository.get()");
                            LOG.error(String.format("repository.get(): path='%s'", getDatasetRequest.getPath()), throwable);
                            res.status(500).send(throwable.getMessage());
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

    static class HttpDeleteResponse {
        final int status;
        final DeleteDatasetResponse response;

        HttpDeleteResponse(int status, DeleteDatasetResponse response) {
            this.status = status;
            this.response = response;
        }
    }

    public void delete(ServerRequest req, ServerResponse res, DeleteDatasetRequest request) {
        Span span = spanFromHttp(req, "delete");
        try {
            String bearerToken = req.headers().value("Authorization")
                    .filter(h -> h.contains("Bearer "))
                    .map(h -> h.substring("Bearer ".length()))
                    .orElse(null);
            DecodedJWT decodedJWT = ofNullable(bearerToken)
                    .map(JWT::decode)
                    .orElseGet(() -> JWT.decode(JWT.create()
                            .withClaim("preferred_username", "unknown")
                            .sign(Algorithm.HMAC256("s3cr3t"))));
            String userId = decodedJWT.getClaim("preferred_username").asString();
            //String userId = decodedJWT.getSubject(); // TODO use subject instead of preferred_username

            repositoryGet(request.getPath(), request.getTimestamp())
                    .flatMapSingle(dataset -> userAccessClient.hasAccess(AccessCheckRequest.newBuilder()
                            .setUserId(userId)
                            .setPath(dataset.getId().getPath())
                            .setPrivilege(Privilege.DELETE.name())
                            .setValuation(dataset.getValuation().name())
                            .setState(dataset.getState().name())
                            .build(), bearerToken)
                            .flatMapSingle(checkResponse -> {
                                if (checkResponse != null && checkResponse.getAllowed()) {
                                    Tracing.restoreTracingContext(req.tracer(), span);
                                    return repository.delete(dataset.getId().getPath(), dataset.getId().getTimestamp())
                                            .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                                            .map(updatedCount -> new HttpDeleteResponse(200, DeleteDatasetResponse.getDefaultInstance()));
                                } else {
                                    return Single.just(new HttpDeleteResponse(403, null));
                                }
                            })
                    )
                    .toOptionalSingle()
                    .subscribe(httpDeleteResponseOpt -> httpDeleteResponseOpt.ifPresentOrElse(httpDeleteResponse -> {
                                Tracing.restoreTracingContext(req.tracer(), span);
                                res.status(httpDeleteResponse.status);
                                if (httpDeleteResponse.response != null) {
                                    res.send(httpDeleteResponse.response);
                                } else {
                                    res.send();
                                }
                            }, () -> res.send()), t -> {
                                try {
                                    Tracing.restoreTracingContext(req.tracer(), span);
                                    logError(span, t, "error in authService.hasAccess()");
                                    LOG.error("error in authService.hasAccess()", t);
                                    res.status(500).send(t.getMessage());
                                } finally {
                                    span.finish();
                                }
                            }
                    );
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

    private void writeDataset(ServerRequest req, ServerResponse res, SignedDataset signedDataset) {
        Span span = spanFromHttp(req, "writeDataset");
        try {
            byte[] signatureBytes = signedDataset.getDatasetMetaAllSignatureBytes().toByteArray();
            byte[] datasetMetaAllBytes = signedDataset.getDatasetMetaAllBytes().toByteArray();

            if (signatureBytes.length == 0 || datasetMetaAllBytes.length == 0) {
                res.status(400).send("Missing dataset metadata or dataset metadata signature");
                return;
            }
            boolean verified = verifier.verify(datasetMetaAllBytes, signatureBytes);
            if (!verified) {
                res.status(401).send("Signature Unauthorized.");
                return;
            }

            DatasetMetaAll datasetMetaAll;
            try {
                datasetMetaAll = DatasetMetaAll.parseFrom(datasetMetaAllBytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }

            String bearerToken = req.headers().value("Authorization")
                    .filter(h -> h.contains("Bearer "))
                    .map(h -> h.substring("Bearer ".length()))
                    .orElse(null);
            DecodedJWT decodedJWT = ofNullable(bearerToken)
                    .map(JWT::decode)
                    .orElseGet(() -> JWT.decode(JWT.create()
                            .withClaim("preferred_username", "unknown")
                            .sign(Algorithm.HMAC256("s3cr3t"))));
            String userIdFromJWT = decodedJWT.getClaim("preferred_username").asString();

            if (!datasetMetaAll.getCreatedBy().equals(userIdFromJWT)) {
                res.status(401).send("createdBy user in signed metadata does not match user in bearer-token");
            }

            Dataset.Builder datasetBuilder = Dataset.newBuilder();
            datasetBuilder
                    .setId(DatasetId.newBuilder()
                            .setPath(datasetMetaAll.getId().getPath())
                            .setTimestamp(Long.parseLong(datasetMetaAll.getId().getVersion()))
                            .build())
                    .setType(ofNullable(datasetMetaAll.getType())
                            .map(t -> Dataset.Type.valueOf(t.name()))
                            .orElse(Dataset.Type.BOUNDED))
                    .setValuation(ofNullable(datasetMetaAll.getValuation())
                            .map(v -> Dataset.Valuation.valueOf(v.name()))
                            .orElseThrow())
                    .setState(ofNullable(datasetMetaAll.getState())
                            .map(s -> Dataset.DatasetState.valueOf(s.name()))
                            .orElseThrow())
                    .setParentUri(datasetMetaAll.getParentUri());
            if (datasetMetaAll.getPseudoConfig() != null) {
                datasetBuilder.setPseudoConfig(PseudoConfig.newBuilder()
                        .addAllVars(datasetMetaAll.getPseudoConfig()
                                .getVarsList()
                                .stream()
                                .map(vpci -> VarPseudoConfigItem.newBuilder()
                                        .setVar(vpci.getVar())
                                        .setPseudoFunc(vpci.getPseudoFunc())
                                        .build())
                                .collect(Collectors.toList()))
                        .build());
            }
            Dataset dataset = datasetBuilder.build();

            // TODO decide whether we need to check access for user again ... probably not needed as signature is valid
            // TODO and user in token is the same user in createdBy field

            repository.create(dataset)
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .subscribe(updatedCount -> {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        res.send();
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error in repository.create()");
                            LOG.error(String.format("repository.create(): dataset-id='%s'", dataset.getId().getPath()), throwable);
                            res.status(505).send();
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
        Span span = spanFromHttp(req, "doGetAll");
        try {
            String pathPart = req.path().param("pathPart");
            String finalPathPart = pathPart != null ? pathPart : "";
            repository.listDatasets(finalPathPart, limit)
                    .timeout(5, TimeUnit.SECONDS, scheduledExecutorService)
                    .collectList()
                    .subscribe(datasets -> {
                        Tracing.restoreTracingContext(req.tracer(), span);

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
                            if (dataset.getPseudoConfig().getVarsCount() > 0) {
                                currentDataset.putObject("pseudoConfig")
                                        .put("vars", dataset.getPseudoConfig().getVarsList().toString());
                            }

                        }

                        res.send(jsonCatalogs.toString());
                        span.finish();
                        res.send();
                        Tracing.traceOutputMessage(span, jsonCatalogs.toString());
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(req.tracer(), span);
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
