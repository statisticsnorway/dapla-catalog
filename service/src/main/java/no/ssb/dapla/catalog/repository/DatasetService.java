package no.ssb.dapla.catalog.repository;

import com.google.protobuf.InvalidProtocolBufferException;
import io.helidon.common.http.Http;
import io.helidon.webserver.*;
import no.ssb.dapla.catalog.protobuf.Dataset;

public class DatasetService implements Service {
    final DatasetRepository repository;

    public DatasetService(DatasetRepository repository){
        this.repository = repository;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{datasetId}", this::doGet);
        rules.put("/{datasetId}", Handler.create(Dataset.class, this::doPut));
        rules.delete("/{datasetId}", this::doDelete);
    }

    private void doGet(ServerRequest request, ServerResponse response){
        String datasetId = request.path().param("datasetId");

        repository.get(datasetId)
                .thenAccept(dataset -> {
                    if(dataset == null){
                        response.status(Http.Status.NOT_FOUND_404).send();
                    }else{
                        response.send(dataset);
                    }
                });

    }

    private void doPut(ServerRequest request, ServerResponse response, Dataset dataset){
        String datasetId = request.path().param("datasetId");
        if(!datasetId.equals(dataset.getId())){
            response.status(Http.Status.BAD_REQUEST_400).send("datasetId in path must match that in body");
        }
        repository.create(dataset);
    }

    private void doDelete(ServerRequest request, ServerResponse response){
        String datasetId = request.path().param("datasetId");
        repository.delete(datasetId);
    }
}
