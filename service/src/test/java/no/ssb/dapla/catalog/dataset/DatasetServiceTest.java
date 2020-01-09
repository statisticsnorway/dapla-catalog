package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.grpc.Channel;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.ResponseHelper;
import no.ssb.dapla.catalog.TestClient;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetResponse;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(IntegrationTestExtension.class)
class DatasetServiceTest {

    @Inject
    Application application;

    @Inject
    Channel channel;

    @Inject
    TestClient testClient;

    @BeforeEach
    public void beforeEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows(DatasetRepository.TABLE_ID);
        application.get(BigtableTableAdminClient.class).dropAllRows(NameIndex.TABLE_ID);
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

    GetByIdDatasetResponse get(String id) {
        return CatalogServiceGrpc.newBlockingStub(channel).getById(GetByIdDatasetRequest.newBuilder().setId(id).build());
    }

    GetByIdDatasetResponse get(String id, long timestamp) {
        return CatalogServiceGrpc.newBlockingStub(channel).getById(GetByIdDatasetRequest.newBuilder().setId(id).setTimestamp(timestamp).build());
    }

    SaveDatasetResponse save(Dataset dataset) {
        return CatalogServiceGrpc.newBlockingStub(channel).save(SaveDatasetRequest.newBuilder().setDataset(dataset).build());
    }

    DeleteDatasetResponse delete(String id) {
        return CatalogServiceGrpc.newBlockingStub(channel).delete(DeleteDatasetRequest.newBuilder().setId(id).build());
    }

    MapNameToIdResponse mapNameToId(String name) {
        return CatalogServiceGrpc.newBlockingStub(channel).mapNameToId(MapNameToIdRequest.newBuilder().addAllName(NamespaceUtils.toComponents(name)).build());
    }

    MapNameToIdResponse mapNameToId(String name, String proposedId) {
        return CatalogServiceGrpc.newBlockingStub(channel).mapNameToId(MapNameToIdRequest.newBuilder().setProposedId(proposedId).addAllName(NamespaceUtils.toComponents(name)).build());
    }

    @Test
    void thatMapToIdWorks() {
        assertThat(mapNameToId("mapToIdWorksTestId1234").getId()).isNullOrEmpty();
        assertThat(mapNameToId("mapToIdWorksTestId1234", "abc").getId()).isEqualTo("abc");
        assertThat(mapNameToId("mapToIdWorksTestId1234", "def").getId()).isEqualTo("abc");
        assertThat(mapNameToId("mapToIdWorksTestId1234").getId()).isEqualTo("abc");
    }

    @Test
    void thatGetDatasetWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("1").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.OUTPUT)
                .setPseudoConfig("pseudo_conf")
                .addLocations("f1")
                .build();
        repositoryCreate(dataset);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setId("2").build())
                        .setValuation(Dataset.Valuation.SENSITIVE)
                        .setState(Dataset.DatasetState.RAW)
                        .setPseudoConfig("pseudo_conf_2")
                        .addLocations("file")
                        .addLocations("file2")
                        .build()
        );

        assertThat(get("1").getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatGetDoesntReturnADatasetWhenOneDoesntExist() {
        assertThat(get("does_not_exist").hasDataset()).isFalse();
    }

    @Test
    void thatGettingAPreviousDatasetWorks() throws InterruptedException {
        Dataset old = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("a_dataset").build())
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig("config")
                .build();
        repositoryCreate(old);

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setId("a_dataset").build())
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .addLocations("a_location")
                        .build()
        );

        assertThat(get("a_dataset", timestamp).getDataset()).isEqualTo(old);
    }

    @Test
    void thatGetPreviousReturnsNothingWhenTimestampIsOld() {
        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setId("dataset_from_after_timestamp").build())
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .addLocations("a_location")
                        .build()
        );
        assertThat(get("dataset_from_after_timestamp", 100L).hasDataset()).isFalse();
    }

    @Test
    void thatGetPreviousReturnsTheLatestDatasetWhenTimestampIsAfterTheLatest() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("dataset_from_before_timestamp").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.PRODUCT)
                .setPseudoConfig("pC")
                .addLocations("some_file")
                .build();
        repositoryCreate(dataset);

        long timestamp = System.currentTimeMillis() + 50;

        assertThat(get("dataset_from_before_timestamp", timestamp).getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatCreateWorks() {
        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("dataset_to_create").build())
                .setValuation(Dataset.Valuation.SENSITIVE)
                .setState(Dataset.DatasetState.OUTPUT)
                .setPseudoConfig("pseudo_config")
                .addLocations("file_location")
                .build();
        save(ds1);
        assertThat(repositoryGet("dataset_to_create")).isEqualTo(ds1);

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("dataset_to_create").build())
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig("another_pseudo_config")
                .addLocations("file_location")
                .addLocations("file_location_2")
                .build();
        save(ds2);
        assertThat(repositoryGet("dataset_to_create")).isEqualTo(ds2);
    }

    @Test
    void thatDeleteWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("dataset_to_delete").build())
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.RAW)
                .addLocations("f")
                .build();
        repositoryCreate(dataset);
        delete(dataset.getId().getId());
        assertThat(repositoryGet("dataset_to_delete")).isNull();
    }

    @Test
    void thatDeleteWorksWhenDatasetDoesntExist() {
        delete("does_not_exist");
    }

    Dataset createDataset(String datasetId, Dataset.DatasetState datasetState, Dataset.Valuation datasetValuation, String pseudoConfig, String location) {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId(datasetId).build())
                .setState(datasetState)
                .setValuation(datasetValuation)
                .setPseudoConfig(pseudoConfig)
                .addLocations(location)
                .build();
        repositoryCreate(dataset);
        return dataset;
    }

    @Test
    void thatGetWorks() {
        Dataset expectedDataset = createDataset("1", Dataset.DatasetState.PRODUCT, Dataset.Valuation.INTERNAL, "pC1", "f1");
        Dataset dataset = testClient.get("/dataset/1", Dataset.class).expect200Ok().body();
        assertEquals(expectedDataset, dataset);
    }

    @Test
    void thatGetNonExistentRoleRespondsWith404NotFound() {
        testClient.get("/dataset/2").expect404NotFound();
    }

    @Test
    void thatPutWorks() {
        Dataset expectedDataset = createDataset("2", Dataset.DatasetState.RAW, Dataset.Valuation.SENSITIVE, "pC2", "f2");
        ResponseHelper<String> helper = testClient.put("/dataset/2", expectedDataset).expect201Created();
        assertEquals("/dataset/2", helper.response().headers().firstValue("Location").orElseThrow());
        Dataset dataset = repositoryGet("2");
        assertEquals(expectedDataset, dataset);
    }

    @Test
    void thatPutReturns400WhenIdsDoesntMatch() {
        Dataset ds = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("an_id").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.PRODUCT)
                .setPseudoConfig("config")
                .addLocations("f")
                .build();
        testClient.put("/dataset/a_different_id", ds).expect400BadRequest();
    }
}
