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
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.dapla.catalog.protobuf.NameAndIdEntry;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.dapla.catalog.protobuf.UnmapNameRequest;
import no.ssb.dapla.catalog.protobuf.UnmapNameResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.List;
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

    UnmapNameResponse unmapName(String name) {
        return CatalogServiceGrpc.newBlockingStub(channel).unmapName(UnmapNameRequest.newBuilder().addAllName(NamespaceUtils.toComponents(name)).build());
    }

    ListByPrefixResponse listByPrefix(String prefix, int limit) {
        return CatalogServiceGrpc.newBlockingStub(channel).listByPrefix(ListByPrefixRequest.newBuilder().setPrefix(prefix).setLimit(limit).build());
    }

    @Test
    void thatMapToIdAndUnmapWorks() {
        assertThat(mapNameToId("mapToIdWorksTestId1234").getId()).isNullOrEmpty();
        assertThat(mapNameToId("mapToIdWorksTestId1234", "abc").getId()).isEqualTo("abc");
        assertThat(mapNameToId("mapToIdWorksTestId1234", "def").getId()).isEqualTo("abc");
        assertThat(mapNameToId("mapToIdWorksTestId1234").getId()).isEqualTo("abc");
        unmapName("mapToIdWorksTestId1234");
        assertThat(mapNameToId("mapToIdWorksTestId1234").getId()).isEqualTo("");
    }

    @Test
    void thatListByPrefixWorks() {
        NameIndex index = application.get(NameIndex.class);
        index.mapNameToId("/another/prefix", "another").join();
        index.mapNameToId("/unit-test/and/other/data", "other").join();
        index.mapNameToId("/unit-test/with/data/1", "1").join();
        index.mapNameToId("/unit-test/with/data/2", "2").join();
        index.mapNameToId("/unit-test/with/data/3", "3").join();
        index.mapNameToId("/unitisgood/forall", "me").join();
        index.mapNameToId("/x-after/and/more/data", "more").join();

        ListByPrefixResponse response = listByPrefix("/unit", 100);

        List<NameAndIdEntry> entries = response.getEntriesList();
        assertThat(entries.size()).isEqualTo(5);
        assertThat(NamespaceUtils.toNamespace(entries.get(0).getNameList())).isEqualTo("/unit-test/and/other/data");
        assertThat(entries.get(0).getId()).isEqualTo("other");
        assertThat(NamespaceUtils.toNamespace(entries.get(1).getNameList())).isEqualTo("/unit-test/with/data/1");
        assertThat(entries.get(1).getId()).isEqualTo("1");
        assertThat(NamespaceUtils.toNamespace(entries.get(2).getNameList())).isEqualTo("/unit-test/with/data/2");
        assertThat(entries.get(2).getId()).isEqualTo("2");
        assertThat(NamespaceUtils.toNamespace(entries.get(3).getNameList())).isEqualTo("/unit-test/with/data/3");
        assertThat(entries.get(3).getId()).isEqualTo("3");
        assertThat(NamespaceUtils.toNamespace(entries.get(4).getNameList())).isEqualTo("/unitisgood/forall");
        assertThat(entries.get(4).getId()).isEqualTo("me");
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
