package no.ssb.dapla.catalog.dataset;

import io.grpc.stub.StreamObserver;
import io.helidon.common.http.Http;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DatasetService extends CatalogServiceGrpc.CatalogServiceImplBase implements Service {

    final DatasetRepository repository;

    public DatasetService(DatasetRepository repository) {
        this.repository = repository;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{datasetId}", this::httpGet);
        rules.put("/{datasetId}", Handler.create(Dataset.class, this::httpPut));
        rules.delete("/{datasetId}", this::httpDelete);
    }

    void httpGet(ServerRequest request, ServerResponse response) {
        String datasetId = request.path().param("datasetId");
        repository.get(datasetId)
                .thenAccept(dataset -> {
                    if (dataset == null) {
                        response.status(Http.Status.NOT_FOUND_404).send();
                    } else {
                        response.send(dataset);
                    }
                });

    }

    void httpPut(ServerRequest request, ServerResponse response, Dataset dataset) {
        String datasetId = request.path().param("datasetId");
        if (!datasetId.equals(dataset.getId())) {
            response.status(Http.Status.BAD_REQUEST_400).send("datasetId in path must match that in body");
        }
        repository.create(dataset);
    }

    void httpDelete(ServerRequest request, ServerResponse response) {
        String datasetId = request.path().param("datasetId");
        repository.delete(datasetId);
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
                    responseObserver.onError(throwable);
                    return null;
                });
    }
}