package no.ssb.dapla.catalog.repository;

import com.google.protobuf.InvalidProtocolBufferException;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.ResponseHelper;
import no.ssb.dapla.catalog.TestClient;
import no.ssb.dapla.catalog.protobuf.Dataset;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatasetServiceTest {
    @Inject
    Application application;

    @Inject
    TestClient testClient;

    Dataset createDataset(String datasetId, Dataset.DatasetState datasetState,  Dataset.Valuation datasetValuation, String location){
        Dataset dataset = Dataset.newBuilder()
                .setId(datasetId)
                .setState(datasetState)
                .setValuation(datasetValuation)
                .addLocations(location)
                .build();
        application.get(DatasetRepository.class).create(dataset);

        return dataset;
    }

    Dataset readDataset(String datasetId) throws InvalidProtocolBufferException {
        return application.get(DatasetRepository.class).get(datasetId).join();
    }

    @Test
    void thatGetWorks() throws InvalidProtocolBufferException {
        Dataset expectedDataset = createDataset("1", Dataset.DatasetState.PRODUCT, Dataset.Valuation.INTERNAL, "f1");
        String body = testClient.get("dataset/1").expect200Ok().body();
        System.out.printf("%s%n", body);
        Dataset dataset = Dataset.parseFrom(body.getBytes());

        assertEquals(expectedDataset, dataset);
    }

    @Test
    void thatGetNonExistentRoleRespondsWith404NotFound(){
        testClient.get("dataset/2").expect404NotFound();
    }

    @Test
    void thatPutWorks() throws InvalidProtocolBufferException {
        Dataset expectedDataset = createDataset("2", Dataset.DatasetState.RAW, Dataset.Valuation.SENSITIVE, "f2");
        ResponseHelper<String> helper = testClient.put("/dataset/2", expectedDataset).expect201Created();
        assertEquals("/dataset/2", helper.response().headers().firstValue("Location").orElseThrow());
        Dataset dataset = readDataset("2");
        assertEquals(expectedDataset, dataset);
    }
}