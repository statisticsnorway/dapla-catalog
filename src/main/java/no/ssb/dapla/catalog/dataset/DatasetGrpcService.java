package no.ssb.dapla.catalog.dataset;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static no.ssb.dapla.catalog.dataset.Tracing.logError;
import static no.ssb.dapla.catalog.dataset.Tracing.spanFromGrpc;

public class DatasetGrpcService extends CatalogServiceGrpc.CatalogServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetGrpcService.class);

    final DatasetRepository repository;
    final NameIndex nameIndex;

    final AuthServiceGrpc.AuthServiceFutureStub authService;

    public DatasetGrpcService(DatasetRepository repository, NameIndex nameIndex, AuthServiceGrpc.AuthServiceFutureStub authService) {
        this.repository = repository;
        this.nameIndex = nameIndex;
        this.authService = authService;
    }

    static class AuthorizationBearer extends CallCredentials {

        private String token;

        AuthorizationBearer(String token) {
            this.token = token;
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

    @Override
    public void mapNameToId(MapNameToIdRequest request, StreamObserver<MapNameToIdResponse> responseObserver) {
        Span span = spanFromGrpc("mapNameToId");
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
                        MapNameToIdResponse response = MapNameToIdResponse.newBuilder().setId(datasetId == null ? "" : datasetId).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
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
        Span span = spanFromGrpc("unmapName");
        try {
            String name = NamespaceUtils.toNamespace(request.getNameList());
            nameIndex.deleteMappingFor(name)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(datasetId -> {
                        UnmapNameResponse response = UnmapNameResponse.newBuilder().build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
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
        Span span = spanFromGrpc("getById");
        try {
            repositoryGet(request.getId(), request.getTimestamp())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(dataset -> {
                        GetByIdDatasetResponse.Builder builder = GetByIdDatasetResponse.newBuilder();
                        if (dataset != null) {
                            builder.setDataset(dataset);
                        }
                        responseObserver.onNext(builder.build());
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
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
        Span span = spanFromGrpc("getByName");
        try {
            nameIndex.mapNameToId(NamespaceUtils.toNamespace(request.getNameList())).thenAccept(id -> {
                repositoryGet(id, request.getTimestamp())
                        .orTimeout(5, TimeUnit.SECONDS)
                        .thenAccept(dataset -> {
                            GetByNameDatasetResponse.Builder builder = GetByNameDatasetResponse.newBuilder();
                            if (dataset != null) {
                                builder.setDataset(dataset);
                            }
                            responseObserver.onNext(builder.build());
                            responseObserver.onCompleted();
                            span.finish();
                        })
                        .exceptionally(throwable -> {
                            try {
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
        Span span = spanFromGrpc("listByPrefix");
        try {
            nameIndex.listByPrefix(request.getPrefix(), 100)
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(entries -> {
                        ListByPrefixResponse response = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
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
        Span span = spanFromGrpc("save");
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
                    .withCallCredentials(new AuthorizationBearer(AuthorizationInterceptor.tokenThreadLocal.get()))
                    .hasAccess(checkRequest);

            Futures.addCallback(hasAccessListenableFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable AccessCheckResponse result) {
                    if (result != null && result.getAllowed()) {
                        repository.create(request.getDataset())
                                .orTimeout(5, TimeUnit.SECONDS)
                                .thenAccept(aVoid -> {
                                    responseObserver.onNext(SaveDatasetResponse.getDefaultInstance());
                                    responseObserver.onCompleted();
                                    span.finish();
                                })
                                .exceptionally(throwable -> {
                                    try {
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
        Span span = spanFromGrpc("delete");
        try {
            repository.delete(request.getId())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(integer -> {
                        responseObserver.onNext(DeleteDatasetResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
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
