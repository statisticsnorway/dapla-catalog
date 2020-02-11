package no.ssb.dapla.catalog.dataset;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetByNameDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByNameDatasetResponse;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.dapla.catalog.protobuf.UnmapNameRequest;
import no.ssb.dapla.catalog.protobuf.UnmapNameResponse;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromGrpc;
import static no.ssb.helidon.application.Tracing.traceOutputMessage;

public class CatalogGrpcService extends CatalogServiceGrpc.CatalogServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogGrpcService.class);

    final DatasetRepository repository;
    final NameIndex nameIndex;

    final AuthServiceGrpc.AuthServiceFutureStub authService;

    public CatalogGrpcService(DatasetRepository repository, NameIndex nameIndex, AuthServiceGrpc.AuthServiceFutureStub authService) {
        this.repository = repository;
        this.nameIndex = nameIndex;
        this.authService = authService;
    }

    @Override
    public void mapNameToId(MapNameToIdRequest request, StreamObserver<MapNameToIdResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "mapNameToId");
        Span span = tracerAndSpan.span();
        try {
            String name = NamespaceUtils.toNamespace(request.getNameList());
            CompletableFuture<String> future;
            if (request.getProposedId().isBlank()) {
                future = nameIndex.mapNameToId(name);
            } else {
                future = nameIndex.mapNameToId(name, request.getProposedId());
            }
            future
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(datasetId -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        MapNameToIdResponse response = MapNameToIdResponse.newBuilder().setId(datasetId == null ? "" : datasetId).build();
                        responseObserver.onNext(traceOutputMessage(span, response));
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in nameIndex.mapNameToId()");
                            LOG.error(String.format("nameIndex.mapNameToId(): name='%s'", name), throwable);
                            responseObserver.onError(throwable);
                            return null;
                        } finally {
                            span.finish();
                        }
                    })
            ;
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
    public void unmapName(UnmapNameRequest request, StreamObserver<UnmapNameResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "unmapName");
        Span span = tracerAndSpan.span();
        try {
            String name = NamespaceUtils.toNamespace(request.getNameList());
            nameIndex.deleteMappingFor(name)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(datasetId -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        UnmapNameResponse response = UnmapNameResponse.newBuilder().build();
                        responseObserver.onNext(traceOutputMessage(span, response));
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in nameIndex.deleteMappingFor()");
                            LOG.error(String.format("nameIndex.deleteMappingFor(): name='%s'", name), throwable);
                            responseObserver.onError(throwable);
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

    @Override
    public void getById(GetByIdDatasetRequest request, StreamObserver<GetByIdDatasetResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "getById");
        Span span = tracerAndSpan.span();
        try {
            repositoryGet(request.getId(), request.getTimestamp())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(dataset -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        GetByIdDatasetResponse.Builder builder = GetByIdDatasetResponse.newBuilder();
                        if (dataset != null) {
                            builder.setDataset(dataset);
                        }
                        responseObserver.onNext(traceOutputMessage(span, builder.build()));
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in repository.get()");
                            LOG.error(String.format("repository.get(): id='%s'", request.getId()), throwable);
                            responseObserver.onError(throwable);
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

    @Override
    public void getByName(GetByNameDatasetRequest request, StreamObserver<GetByNameDatasetResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "getByName");
        Span span = tracerAndSpan.span();
        try {
            nameIndex.mapNameToId(NamespaceUtils.toNamespace(request.getNameList())).thenAccept(id -> {
                repositoryGet(id, request.getTimestamp())
                        .orTimeout(5, TimeUnit.SECONDS)
                        .thenAccept(dataset -> {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            GetByNameDatasetResponse.Builder builder = GetByNameDatasetResponse.newBuilder();
                            if (dataset != null) {
                                builder.setDataset(dataset);
                            }
                            responseObserver.onNext(traceOutputMessage(span, builder.build()));
                            responseObserver.onCompleted();
                            span.finish();
                        })
                        .exceptionally(throwable -> {
                            try {
                                Tracing.restoreTracingContext(tracerAndSpan);
                                logError(span, throwable, "error in repository.get()");
                                LOG.error(String.format("repository.get(): name='%s', which was mapped to id '%s'", request.getNameList(), id), throwable);
                                responseObserver.onError(throwable);
                                return null;
                            } finally {
                                span.finish();
                            }
                        });
            }).exceptionally(throwable -> {
                try {
                    Tracing.restoreTracingContext(tracerAndSpan);
                    logError(span, throwable, "error in nameIndex.mapNameToId()");
                    LOG.error(String.format("nameIndex.mapNameToId(): name='%s'", request.getNameList()), throwable);
                    responseObserver.onError(throwable);
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

    private CompletableFuture<Dataset> repositoryGet(String id, long timestamp) {
        if (id == null) {
            return CompletableFuture.completedFuture(null);
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
            nameIndex.listByPrefix(request.getPrefix(), 100)
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(entries -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        ListByPrefixResponse response = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                        responseObserver.onNext(traceOutputMessage(span, response));
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in nameIndex.listByPrefix()");
                            LOG.error(String.format("nameIndex.listByPrefix(): prefix='%s'", request.getPrefix()), throwable);
                            responseObserver.onError(throwable);
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

    @Override
    public void save(SaveDatasetRequest request, StreamObserver<SaveDatasetResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "save");
        Span span = tracerAndSpan.span();
        try {
            String userId = request.getUserId();
            Dataset dataset = request.getDataset();

            AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                    .setUserId(userId)
                    .setNamespace(NamespaceUtils.toNamespace(dataset.getId().getNameList()))
                    .setPrivilege(Role.Privilege.CREATE.name())
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
                                .orTimeout(5, TimeUnit.SECONDS)
                                .thenAccept(aVoid -> {
                                    Tracing.restoreTracingContext(tracerAndSpan);
                                    responseObserver.onNext(traceOutputMessage(span, SaveDatasetResponse.getDefaultInstance()));
                                    responseObserver.onCompleted();
                                    span.finish();
                                })
                                .exceptionally(throwable -> {
                                    try {
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        logError(span, throwable, "error in repository.create()");
                                        LOG.error(String.format("repository.create(): dataset-id='%s'", request.getDataset().getId().getId()), throwable);
                                        responseObserver.onError(throwable);
                                        return null;
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
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(integer -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        responseObserver.onNext(traceOutputMessage(span, DeleteDatasetResponse.getDefaultInstance()));
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error in repository.delete()");
                            LOG.error(String.format("repository.delete(): dataset-id='%s'", request.getId()), throwable);
                            responseObserver.onError(throwable);
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
}
