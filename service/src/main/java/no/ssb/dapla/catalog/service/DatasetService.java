package no.ssb.dapla.catalog.service;

import io.grpc.stub.StreamObserver;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.dapla.catalog.repository.DatasetRepository;

import java.util.concurrent.TimeUnit;

public class DatasetService extends CatalogServiceGrpc.CatalogServiceImplBase {

    final DatasetRepository repository;

    public DatasetService(DatasetRepository repository) {
        this.repository = repository;
    }

    @Override
    public void get(GetDatasetRequest request, StreamObserver<GetDatasetResponse> responseObserver) {
        if (request.getTimestamp() > 0) {
            repository.get(request.getId(), request.getTimestamp())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(dataset -> {
                        responseObserver.onNext(GetDatasetResponse.newBuilder().setDataset(dataset).build());
                        responseObserver.onCompleted();
                    })
                    .exceptionally(throwable -> {
                        responseObserver.onError(throwable);
                        return null;
                    });
        } else {
            repository.get(request.getId())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(dataset -> {
                        responseObserver.onNext(GetDatasetResponse.newBuilder().setDataset(dataset).build());
                        responseObserver.onCompleted();
                    })
                    .exceptionally(throwable -> {
                        responseObserver.onError(throwable);
                        return null;
                    });
        }
    }

    @Override
    public void save(SaveDatasetRequest request, StreamObserver<SaveDatasetResponse> responseObserver) {
        repository.create(request.getDataset())
                .orTimeout(5, TimeUnit.SECONDS)
                .thenAccept(aVoid -> {
                    responseObserver.onNext(SaveDatasetResponse.newBuilder().build());
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
                    responseObserver.onNext(DeleteDatasetResponse.newBuilder().build());
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    responseObserver.onError(throwable);
                    return null;
                });
    }
}
