package no.ssb.dapla.catalog.dataset;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import io.reactivex.Maybe;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Privilege;
import no.ssb.dapla.catalog.protobuf.*;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromGrpc;
import static no.ssb.helidon.application.Tracing.traceOutputMessage;

public class CatalogGrpcService extends CatalogServiceGrpc.CatalogServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogGrpcService.class);

    final DatasetRepository repository;

    final AuthServiceGrpc.AuthServiceFutureStub authService;

    public CatalogGrpcService(DatasetRepository repository, AuthServiceGrpc.AuthServiceFutureStub authService) {
        this.repository = repository;
        this.authService = authService;
    }

    @Override
    public void get(GetDatasetRequest request, StreamObserver<GetDatasetResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "get");
        Span span = tracerAndSpan.span();
        try {
            repositoryGet(request.getPath(), request.getTimestamp())
                    .timeout(5, TimeUnit.SECONDS)
                    .subscribe(dataset -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        GetDatasetResponse.Builder builder = GetDatasetResponse.newBuilder()
                                .setDataset(dataset);
                        responseObserver.onNext(traceOutputMessage(span, builder.build()));
                        responseObserver.onCompleted();
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in repository.get()");
                            LOG.error(String.format("repository.get(): path='%s'", request.getPath()), throwable);
                            responseObserver.onError(throwable);
                        } finally {
                            span.finish();
                        }
                    }, () -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        responseObserver.onNext(traceOutputMessage(span, GetDatasetResponse.newBuilder().build()));
                        responseObserver.onCompleted();
                        span.finish();
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

    private Maybe<Dataset> repositoryGet(String id, long timestamp) {
        if (id == null) {
            return Maybe.empty();
        }
        if (timestamp > 0) {
            return repository.get(id, timestamp);
        }
        return repository.get(id);
    }

    @Override
    public void listByPrefix(ListByPrefixRequest request, StreamObserver<ListByPrefixResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "listByPrefix");
        Span span = tracerAndSpan.span();
        try {
            repository.listByPrefix(request.getPrefix(), 100)
                    .timeout(5, TimeUnit.SECONDS)
                    .toList()
                    .subscribe(entries -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        ListByPrefixResponse response = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                        responseObserver.onNext(traceOutputMessage(span, response));
                        responseObserver.onCompleted();
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in nameIndex.listByPrefix()");
                            LOG.error(String.format("nameIndex.listByPrefix(): prefix='%s'", request.getPrefix()), throwable);
                            responseObserver.onError(throwable);
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

    @Override
    public void save(SaveDatasetRequest request, StreamObserver<SaveDatasetResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "save");
        Span span = tracerAndSpan.span();
        try {
            String userId = request.getUserId();
            Dataset dataset = request.getDataset();

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
                        repository.create(request.getDataset())
                                .timeout(5, TimeUnit.SECONDS)
                                .subscribe(updatedCount -> {
                                    Tracing.restoreTracingContext(tracerAndSpan);
                                    responseObserver.onNext(traceOutputMessage(span, SaveDatasetResponse.getDefaultInstance()));
                                    responseObserver.onCompleted();
                                    span.finish();
                                }, throwable -> {
                                    try {
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        logError(span, throwable, "error in repository.create()");
                                        LOG.error(String.format("repository.create(): dataset-id='%s'", request.getDataset().getId().getPath()), throwable);
                                        responseObserver.onError(throwable);
                                    } finally {
                                        span.finish();
                                    }
                                });
                    } else {
                        responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
                        span.finish();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    try {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        logError(span, t, "error in authService.hasAccess()");
                        LOG.error("error in authService.hasAccess()", t);
                        responseObserver.onError(t);
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

    @Override
    public void delete(DeleteDatasetRequest request, StreamObserver<DeleteDatasetResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "delete");
        Span span = tracerAndSpan.span();
        try {
            repository.delete(request.getId())
                    .timeout(5, TimeUnit.SECONDS)
                    .subscribe(updatedCount -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        responseObserver.onNext(traceOutputMessage(span, DeleteDatasetResponse.getDefaultInstance()));
                        responseObserver.onCompleted();
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in repository.delete()");
                            LOG.error(String.format("repository.delete(): dataset-id='%s'", request.getId()), throwable);
                            responseObserver.onError(throwable);
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

    @Override
    public void pollute(PolluteDatasetRequest request, StreamObserver<PolluteDatasetResponse> responseObserver){
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "pollute");
        Span span = tracerAndSpan.span();

        try {
            repository.setPathDirty(request.getPath(), Dataset.IsDirty.DIRTY)
                    .timeout(5, TimeUnit.SECONDS)
                    .subscribe(success -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        responseObserver.onNext(traceOutputMessage(span, PolluteDatasetResponse.getDefaultInstance()));
                        responseObserver.onCompleted();
                        span.finish();
                    }, throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "eerror in repository.pollute()");
                            LOG.error(String.format("repository.pollute(): dataset-path='%s'", request.getPath(), throwable));
                            responseObserver.onError(throwable);
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
