package no.ssb.dapla.catalog.dataset;

import io.grpc.stub.StreamObserver;
import io.helidon.common.http.Http;
import io.helidon.common.http.MediaType;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.catalog.JacksonUtils;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DatasetService extends CatalogServiceGrpc.CatalogServiceImplBase implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetService.class);

    final DatasetRepository repository;

    public DatasetService(DatasetRepository repository) {
        this.repository = repository;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{datasetId}", this::httpGet);
        rules.put("/{datasetId}", Handler.create(String.class, this::httpPut));
        rules.delete("/{datasetId}", this::httpDelete);
    }

    void httpGet(ServerRequest request, ServerResponse response) {
        String datasetId = request.path().param("datasetId");
        repository.get(datasetId)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(dataset -> {
                    if (dataset == null) {
                        response.status(Http.Status.NOT_FOUND_404).send();
                    } else {
                        String json = JacksonUtils.toString(dataset);
                        response.headers().contentType(MediaType.APPLICATION_JSON);
                        response.send(json);
                    }
                })
                .exceptionally(t -> {
                    LOG.error(String.format("While serving %s uri: %s", request.method().name(), request.uri()), t);
                    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    return null;
                });
    }

    void httpPut(ServerRequest request, ServerResponse response, String datasetJson) {
        String datasetId = request.path().param("datasetId");
        Dataset dataset = JacksonUtils.toPojo(datasetJson, Dataset.class);
        if (!datasetId.equals(dataset.getId())) {
            response.status(Http.Status.BAD_REQUEST_400).send("datasetId in path must match that in body");
        }
        repository.create(dataset)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenRun(() -> {
                    response.headers().add("Location", "/dataset/" + datasetId);
                    response.status(Http.Status.CREATED_201).send();
                })
                .exceptionally(t -> {
                    LOG.error(String.format("While serving %s uri: %s", request.method().name(), request.uri()), t);
                    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    return null;
                });
    }

    void httpDelete(ServerRequest request, ServerResponse response) {
        String datasetId = request.path().param("datasetId");
        repository.delete(datasetId)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenRun(response::send)
                .exceptionally(t -> {
                    LOG.error(String.format("While serving %s uri: %s", request.method().name(), request.uri()), t);
                    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    return null;
                });
    }

    @Override
    public void get(GetDatasetRequest request, StreamObserver<GetDatasetResponse> responseObserver) {
        repositoryGet(request)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(dataset -> {
                    GetDatasetResponse.Builder builder = GetDatasetResponse.newBuilder();
                    if (dataset != null) {
                        builder.setDataset(dataset);
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc get for dataset-id %s", request.getId()), throwable);
                    responseObserver.onError(throwable);
                    return null;
                });
    }

    private CompletableFuture<Dataset> repositoryGet(GetDatasetRequest request) {
        if (request.getTimestamp() > 0) {
            return repository.get(request.getId(), request.getTimestamp());
        }
        return repository.get(request.getId());
    }

    @Override
    public void save(SaveDatasetRequest request, StreamObserver<SaveDatasetResponse> responseObserver) {
        repository.create(request.getDataset())
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(aVoid -> {
                    responseObserver.onNext(SaveDatasetResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving grpc save for dataset-id %s", request.getDataset().getId()), throwable);
                    responseObserver.onError(throwable);
                    return null;
                });
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
