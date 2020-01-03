package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.grpc.Channel;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.DatasetAssert;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.ResponseHelper;
import no.ssb.dapla.catalog.TestClient;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(IntegrationTestExtension.class)
class DatasetServiceTest {

    @Inject
    Application application;

    @Inject
    Channel channel;

    @Inject
    TestClient testClient;

    @AfterEach
    public void afterEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows("dataset");
    }

    void repositoryCreate(Dataset dataset) {
        try {
            application.get(DatasetRepository.class).create(dataset).get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    Dataset repositoryGet(String id) {
        try {
            return application.get(DatasetRepository.class).get(id).get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    GetDatasetResponse get(String id) {
        return CatalogServiceGrpc.newBlockingStub(channel).get(GetDatasetRequest.newBuilder().setId(id).build());
    }

    GetDatasetResponse get(String id, long timestamp) {
        return CatalogServiceGrpc.newBlockingStub(channel).get(GetDatasetRequest.newBuilder().setId(id).setTimestamp(timestamp).build());
    }

    SaveDatasetResponse save(Dataset dataset) {
        return CatalogServiceGrpc.newBlockingStub(channel).save(SaveDatasetRequest.newBuilder().setDataset(dataset).build());
    }

    DeleteDatasetResponse delete(String id) {
        return CatalogServiceGrpc.newBlockingStub(channel).delete(DeleteDatasetRequest.newBuilder().setId(id).build());
    }

    @Test
    void thatGetDatasetWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId("1")
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.OUTPUT)
                .addLocations("f1")
                .build();
        repositoryCreate(dataset);


        repositoryCreate(
                Dataset.newBuilder()
                        .setId("2")
                        .setValuation(Dataset.Valuation.SENSITIVE)
                        .setState(Dataset.DatasetState.RAW)
                        .addLocations("file")
                        .addLocations("file2")
                        .build()
        );

        DatasetAssert.assertThat(get("1").getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatGetDoesntReturnADatasetWhenOneDoesntExist() {
        Assertions.assertThat(get("does_not_exist").hasDataset()).isFalse();
    }

    @Test
    void thatGettingAPreviousDatasetWorks() throws InterruptedException {
        Dataset old = Dataset.newBuilder()
                .setId("a_dataset")
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .build();
        repositoryCreate(old);

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId("a_dataset")
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .addLocations("a_location")
                        .build()
        );

        DatasetAssert.assertThat(get("a_dataset", timestamp).getDataset()).isEqualTo(old);
    }

    @Test
    void thatGetPreviousReturnsNothingWhenTimestampIsOld() {
        repositoryCreate(
                Dataset.newBuilder()
                        .setId("dataset_from_after_timestamp")
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .addLocations("a_location")
                        .build()
        );
        Assertions.assertThat(get("dataset_from_after_timestamp", 100L).hasDataset()).isFalse();
    }

    @Test
    void thatGetPreviousReturnsTheLatestDatasetWhenTimestampIsAfterTheLatest() {
        Dataset dataset = Dataset.newBuilder()
                .setId("dataset_from_before_timestamp")
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.PRODUCT)
                .addLocations("some_file")
                .build();
        repositoryCreate(dataset);

        long timestamp = System.currentTimeMillis() + 50;

        DatasetAssert.assertThat(get("dataset_from_before_timestamp", timestamp).getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatCreateWorks() {
        Dataset ds1 = Dataset.newBuilder()
                .setId("dataset_to_create")
                .setValuation(Dataset.Valuation.SENSITIVE)
                .setState(Dataset.DatasetState.OUTPUT)
                .addLocations("file_location")
                .build();
        save(ds1);
        DatasetAssert.assertThat(repositoryGet("dataset_to_create")).isEqualTo(ds1);

        Dataset ds2 = Dataset.newBuilder()
                .setId("dataset_to_create")
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .addLocations("file_location")
                .addLocations("file_location_2")
                .build();
        save(ds2);
        DatasetAssert.assertThat(repositoryGet("dataset_to_create")).isEqualTo(ds2);
    }

    @Test
    void thatDeleteWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId("dataset_to_delete")
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.RAW)
                .addLocations("f")
                .build();
        repositoryCreate(dataset);
        delete(dataset.getId());
        DatasetAssert.assertThat(repositoryGet("dataset_to_delete")).isNull();
    }

    @Test
    void thatDeleteWorksWhenDatasetDoesntExist() {
        delete("does_not_exist");
    }


    Dataset createDataset(String datasetId, Dataset.DatasetState datasetState, Dataset.Valuation datasetValuation, String location) {
        Dataset dataset = Dataset.newBuilder()
                .setId(datasetId)
                .setState(datasetState)
                .setValuation(datasetValuation)
                .addLocations(location)
                .build();
        application.get(DatasetRepository.class).create(dataset);

        return dataset;
    }

    Dataset readDataset(String datasetId) {
        return application.get(DatasetRepository.class).get(datasetId).join();
    }

    @Test
    void thatGetWorks() {
        Dataset expectedDataset = createDataset("1", Dataset.DatasetState.PRODUCT, Dataset.Valuation.INTERNAL, "f1");
        Dataset dataset = testClient.get("/dataset/1", Dataset.class, Dataset.newBuilder()).expect200Ok().body();
        assertEquals(expectedDataset, dataset);
    }

    @Test
    void thatGetNonExistentRoleRespondsWith404NotFound() {
        testClient.get("/dataset/2").expect404NotFound();
    }

    @Test
    void thatPutWorks() {
        Dataset expectedDataset = createDataset("2", Dataset.DatasetState.RAW, Dataset.Valuation.SENSITIVE, "f2");
        ResponseHelper<String> helper = testClient.put("/dataset/2", expectedDataset).expect201Created();
        assertEquals("/dataset/2", helper.response().headers().firstValue("Location").orElseThrow());
        Dataset dataset = readDataset("2");
        assertEquals(expectedDataset, dataset);
    }
}