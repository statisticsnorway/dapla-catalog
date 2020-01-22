package no.ssb.dapla.catalog.dataset;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.helidon.common.http.Http;
import io.helidon.common.http.MediaType;
import io.helidon.webserver.Handler;
import io.helidon.webserver.RequestHeaders;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
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
import no.ssb.helidon.application.LogUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class DatasetService extends CatalogServiceGrpc.CatalogServiceImplBase implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetService.class);

    final DatasetRepository repository;
    final NameIndex nameIndex;

    final AuthServiceGrpc.AuthServiceFutureStub authService;

    public DatasetService(DatasetRepository repository, NameIndex nameIndex, AuthServiceGrpc.AuthServiceFutureStub authService) {
        this.repository = repository;
        this.nameIndex = nameIndex;
        this.authService = authService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{datasetId}", this::httpGet);
        rules.put("/{datasetId}", Handler.create(Dataset.class, this::httpPut));
        rules.delete("/{datasetId}", this::httpDelete);
    }

    void httpGet(ServerRequest request, ServerResponse response) {
        LogUtils.trace(LOG, "httpGet", request);
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
                })
                .exceptionally(t -> {
                    LOG.error(String.format("While serving %s uri: %s", request.method().name(), request.uri()), t);
                    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    return null;
                });
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
                return s.split(" ")[1];
            }).orElse("");
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

    void httpPut(ServerRequest request, ServerResponse response, Dataset dataset) {
        LogUtils.trace(LOG, "httpPut", request, dataset);
        String datasetId = request.path().param("datasetId");
        Optional<String> userId = request.queryParams().first("userId");

        if (!datasetId.equals(dataset.getId().getId())) {
            response.status(Http.Status.BAD_REQUEST_400).send("datasetId in path must match that in body");
            return;
        }

        if (userId.isEmpty()) {
            response.status(Http.Status.BAD_REQUEST_400).send("Expected 'userId'");
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
                if (result != null && result.getAllowed()) {
                    CompletableFuture<Void> future = repository.create(dataset);
                    if (!request.queryParams().first("notimeout").isPresent()) {
                        future.orTimeout(5, TimeUnit.SECONDS);
                    }
                    future
                            .thenRun(() -> {
                                response.headers().add("Location", "/dataset/" + datasetId);
                                response.status(Http.Status.CREATED_201).send();
                            })
                            .exceptionally(t -> {
                                LOG.error(String.format("While serving %s uri: %s", request.method().name(), request.uri()), t);
                                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                                return null;
                            });

                    return;
                }
                response.status(Http.Status.FORBIDDEN_403).send();
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to get dataset meta", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            }
        }, MoreExecutors.directExecutor());
    }

    void httpDelete(ServerRequest request, ServerResponse response) {
        LogUtils.trace(LOG, "httpDelete", request);
        String datasetId = request.path().param("datasetId");
        CompletableFuture<Integer> future = repository.delete(datasetId);
        if (!request.queryParams().first("notimeout").isPresent()) {
            future.orTimeout(5, TimeUnit.SECONDS);
        }
        future
                .thenRun(response::send)
                .exceptionally(t -> {
                    LOG.error(String.format("While serving %s uri: %s", request.method().name(), request.uri()), t);
                    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    return null;
                });
    }

    @Override
    public void mapNameToId(MapNameToIdRequest request, StreamObserver<MapNameToIdResponse> responseObserver) {
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
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc mapNameToId for name: %s", name), throwable);
                    responseObserver.onError(throwable);
                    return null;
                })
        ;
    }

    @Override
    public void unmapName(UnmapNameRequest request, StreamObserver<UnmapNameResponse> responseObserver) {
        String name = NamespaceUtils.toNamespace(request.getNameList());
        nameIndex.deleteMappingFor(name)
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(datasetId -> {
                    UnmapNameResponse response = UnmapNameResponse.newBuilder().build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc unmapName for name: %s", name), throwable);
                    responseObserver.onError(throwable);
                    return null;
                });
    }

    @Override
    public void getById(GetByIdDatasetRequest request, StreamObserver<GetByIdDatasetResponse> responseObserver) {
        repositoryGet(request.getId(), request.getTimestamp())
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(dataset -> {
                    GetByIdDatasetResponse.Builder builder = GetByIdDatasetResponse.newBuilder();
                    if (dataset != null) {
                        builder.setDataset(dataset);
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc getById for id: %s", request.getId()), throwable);
                    responseObserver.onError(throwable);
                    return null;
                });
    }

    @Override
    public void getByName(GetByNameDatasetRequest request, StreamObserver<GetByNameDatasetResponse> responseObserver) {
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
                    })
                    .exceptionally(throwable -> {
                        LOG.error(String.format("While serving grpc getByName for name: %s, which was mapped to id %s", request.getNameList(), id), throwable);
                        responseObserver.onError(throwable);
                        return null;
                    });
        }).exceptionally(throwable -> {
            LOG.error(String.format("While serving grpc getByName for name: %s", request.getNameList()), throwable);
            responseObserver.onError(throwable);
            return null;
        });
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
        nameIndex.listByPrefix(request.getPrefix(), 100)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(entries -> {
                    ListByPrefixResponse response = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc listByPrefix for prefix: %s", request.getPrefix()), throwable);
                    responseObserver.onError(throwable);
                    return null;
                });
    }

    @Override
    public void save(SaveDatasetRequest request, StreamObserver<SaveDatasetResponse> responseObserver) {
        String userId = request.getUserId();
        Dataset dataset = request.getDataset();

        AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                .setUserId(userId)
                .setNamespace(NamespaceUtils.toNamespace(dataset.getId().getNameList()))
                .setPrivilege(Role.Privilege.CREATE.name())
                .setValuation(dataset.getValuation().name())
                .setState(dataset.getState().name())
                .build();

        ListenableFuture<AccessCheckResponse> hasAccessListenableFuture = authService.hasAccess(checkRequest);

        Futures.addCallback(hasAccessListenableFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable AccessCheckResponse result) {
                if (result != null && result.getAllowed()) {
                    repository.create(request.getDataset())
                            .orTimeout(5, TimeUnit.SECONDS)
                            .thenAccept(aVoid -> {
                                responseObserver.onNext(SaveDatasetResponse.getDefaultInstance());
                                responseObserver.onCompleted();
                            })
                            .exceptionally(throwable -> {
                                LOG.error(String.format("While serving grpc save for dataset-id %s", request.getDataset().getId().getId()), throwable);
                                responseObserver.onError(throwable);
                                return null;
                            });
                } else {
                    responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
                }
                return;
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to do access check", t);
                responseObserver.onError(t);
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void delete(DeleteDatasetRequest request, StreamObserver<DeleteDatasetResponse> responseObserver) {
        repository.delete(request.getId())
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(integer -> {
                    responseObserver.onNext(DeleteDatasetResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc delete for dataset-id %s", request.getId()), throwable);
                    responseObserver.onError(throwable);
                    return null;
                });
    }
}
