package no.ssb.dapla.catalog.dataset;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.helidon.common.http.Http;
import io.helidon.common.http.MediaType;
import io.helidon.webserver.Handler;
import io.helidon.webserver.RequestHeaders;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.protobuf.Dataset;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromHttp;

public class DatasetHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetHttpService.class);

    final DatasetRepository repository;
    final NameIndex nameIndex;

    final AuthServiceGrpc.AuthServiceFutureStub authService;

    public DatasetHttpService(DatasetRepository repository, NameIndex nameIndex, AuthServiceGrpc.AuthServiceFutureStub authService) {
        this.repository = repository;
        this.nameIndex = nameIndex;
        this.authService = authService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{datasetId}", this::read);
        rules.put("/{datasetId}", Handler.create(Dataset.class, this::write));
        rules.delete("/{datasetId}", this::delete);
    }

    void read(ServerRequest request, ServerResponse response) {
        Span span = spanFromHttp(request, "read-dataset");
        try {
            String datasetId = request.path().param("datasetId");
            CompletableFuture<Dataset> future = repository.get(datasetId);
            if (!request.queryParams().first("notimeout").isPresent()) {
                future.orTimeout(5, TimeUnit.SECONDS);
            }
            future
                    .thenAccept(dataset -> {
                        if (dataset == null) {
                            response.status(Http.Status.NOT_FOUND_404).send();
                        } else {
                            response.headers().contentType(MediaType.APPLICATION_JSON);
                            response.send(dataset);
                        }
                        span.finish();
                    })
                    .exceptionally(t -> {
                        try {
                            logError(span, t, "error in repository.get(datasetId)");
                            LOG.error(String.format("repository.get(datasetId): datasetId='%s'", datasetId), t);
                            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                            return null;
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

    void write(ServerRequest request, ServerResponse response, Dataset dataset) {
        Span span = spanFromHttp(request, "write-dataset");
        try {
            String datasetId = request.path().param("datasetId");
            Optional<String> userId = request.queryParams().first("userId");

            if (!datasetId.equals(dataset.getId().getId())) {
                response.status(Http.Status.BAD_REQUEST_400).send("datasetId in path must match that in body");
                span.finish();
                return;
            }

            if (userId.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Expected 'userId'");
                span.finish();
                return;
            }

            AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                    .setUserId(userId.get())
                    .setNamespace(NamespaceUtils.toNamespace(dataset.getId().getNameList()))
                    .setPrivilege(Role.Privilege.CREATE.name())
                    .setValuation(dataset.getValuation().name())
                    .setState(dataset.getState().name())
                    .build();

            AuthorizationBearer authorizationBearer = AuthorizationBearer.from(request.headers());

            ListenableFuture<AccessCheckResponse> hasAccessListenableFuture = authService.withCallCredentials(authorizationBearer).hasAccess(checkRequest);

            Futures.addCallback(hasAccessListenableFuture, new FutureCallback<>() {

                @Override
                public void onSuccess(@Nullable AccessCheckResponse result) {
                    try {
                        if (result != null && result.getAllowed()) {
                            CompletableFuture<Void> future = repository.create(dataset);
                            if (!request.queryParams().first("notimeout").isPresent()) {
                                future.orTimeout(5, TimeUnit.SECONDS);
                            }
                            future
                                    .thenRun(() -> {
                                        response.headers().add("Location", "/dataset/" + datasetId);
                                        response.status(Http.Status.CREATED_201).send();
                                        span.finish();
                                    })
                                    .exceptionally(t -> {
                                        try {
                                            logError(span, t, "error in repository.create(dataset)");
                                            LOG.error(String.format("error in repository.create(dataset): dataset=%s", dataset.toString()), t);
                                            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                                            return null;
                                        } finally {
                                            span.finish();
                                        }
                                    });
                            return;
                        }
                        response.status(Http.Status.FORBIDDEN_403).send();
                    } catch (RuntimeException | Error e) {
                        logError(span, e, "error in authService.hasAccess() : onSuccess()");
                        LOG.error("authService.hasAccess() : onSuccess()", e);
                        span.finish();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    try {
                        logError(span, t, "authService.hasAccess() : onFailure()");
                        LOG.error("authService.hasAccess() threw exception", t);
                        response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    } finally {
                        span.finish();
                    }
                }
            }, MoreExecutors.directExecutor());
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

    void delete(ServerRequest request, ServerResponse response) {
        Span span = spanFromHttp(request, "delete-dataset");
        try {
            String datasetId = request.path().param("datasetId");
            CompletableFuture<Integer> future = repository.delete(datasetId);
            if (!request.queryParams().first("notimeout").isPresent()) {
                future.orTimeout(5, TimeUnit.SECONDS);
            }
            future
                    .thenRun(() -> {
                        response.send();
                        span.finish();
                    })
                    .exceptionally(t -> {
                        try {
                            logError(span, t, "error in repository.delete(datasetId)");
                            LOG.error(String.format("repository.delete(datasetId): datasetId='%s'", datasetId), t);
                            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                            return null;
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

    static class AuthorizationBearer extends CallCredentials {
        private String token;

        AuthorizationBearer(String token) {
            this.token = token;
        }

        static AuthorizationBearer from(RequestHeaders headers) {
            String token = headers.first("Authorization").map(s -> {
                if (Strings.isNullOrEmpty(s) || !s.startsWith("Bearer ")) {
                    return "";
                }
                return s.substring("Bearer ".length());
            }).orElse("no-token");
            return new AuthorizationBearer(token);
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), String.format("Bearer %s", token));
            appExecutor.execute(() -> applier.apply(metadata));
        }

        @Override
        public void thisUsesUnstableApi() {
        }
    }
}
