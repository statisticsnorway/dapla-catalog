package no.ssb.dapla.catalog.dataset;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
import no.ssb.dapla.catalog.protobuf.VarPseudoConfigItem;
import no.ssb.dapla.dataset.api.DatasetMetaAll;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.MockRegistryConfig;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(IntegrationTestExtension.class)
@MockRegistryConfig(CatalogMockRegistry.class)
class CatalogHttpServiceTest {

    @Inject
    CatalogApplication application;

    @Inject
    TestClient client;

    String char256 = "fake_signature_of_256_lengthdapla_testing_fake_sinagure of 228 characters length is a long string of chars that is used for testing a fake signature of not importance, ... now it is already 165 chars and i only need to create another 10s of chars to fill .";

    static final MetadataSigner metadataSigner = new MetadataSigner(
            "PKCS12",
            "src/test/resources/metadata-signer_keystore.p12",
            "dataAccessKeyPair",
            "changeit".toCharArray(),
            "SHA256withRSA"
    );

    @BeforeEach
    public void beforeEach() {
        application.get(DatasetRepository.class).deleteAllDatasets().await(30, TimeUnit.SECONDS);
    }

    void repositoryCreate(String path, Instant time) {
        var dataset = Dataset.newBuilder().setId(
                DatasetId.newBuilder()
                        .setPath(path)
                        .setTimestamp(time.toEpochMilli())
                        .build()
        ).build();
        repositoryCreate(dataset);
    }

    void repositoryCreate(String path) {
        var dataset = Dataset.newBuilder().setId(DatasetId.newBuilder().setPath(path).build()).build();
        repositoryCreate(dataset);
    }

    void repositoryCreate(Dataset dataset) {
        application.get(DatasetRepository.class).create(dataset).await(3, TimeUnit.SECONDS);
    }

    @Test
    void thatCatalogGetEmptyList() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        String catalogJson = client.get("/catalog").expect200Ok().body();
        JsonNode actual = mapper.readTree(catalogJson);

        ObjectNode expected = mapper.createObjectNode();
        expected.putArray("catalogs");
        assertEquals(expected, actual);
    }

    @Test
    void thatCatalogSaveDataset() {
        DatasetMetaAll datasetMetaAll = createDatasetMetaAll(0);
        byte[] signature = metadataSigner.sign(datasetMetaAll.toByteArray());
        byte[] datasetMetaAllBytes = datasetMetaAll.toByteArray();

        String[] headers = new String[]{"Authorization", "Bearer " + JWT.create().withClaim("preferred_username", "user")
                .sign(Algorithm.HMAC256("secret"))};

        //Authorized user
        SignedDataset signedDataset = SignedDataset.newBuilder()
                .setDatasetMetaAllBytes(ByteString.copyFrom(datasetMetaAllBytes))
                .setDatasetMetaAllSignatureBytes(ByteString.copyFrom(signature))
                .build();
        client.postAsJson("/catalog/write", signedDataset, headers).expect200Ok();

        // fake signature
        SignedDataset signedDataset2 = SignedDataset.newBuilder()
                .setDatasetMetaAllBytes(ByteString.copyFrom(datasetMetaAllBytes))
                .setDatasetMetaAllSignatureBytes(ByteString.copyFrom(char256.getBytes()))
                .build();
        Assertions.assertEquals(401, client.postAsJson("/catalog/write", signedDataset2, headers).response().statusCode());

        // missing metadata and signature
        SignedDataset signedDataset3 = SignedDataset.newBuilder()
                .build();
        Assertions.assertEquals(400, client.postAsJson("/catalog/write", signedDataset3, headers).response().statusCode());
    }

    @Test
    void thatCatalogGetAllDatasets() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        // Used to recreate object in assertions.
        var now1 = Instant.now();
        var now2 = Instant.now();
        var now3 = Instant.now();
        var now4 = Instant.now();

        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path1/dataset1")
                        .setTimestamp(now1.toEpochMilli())
                        .build())
                .setType(Dataset.Type.BOUNDED)
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.INPUT)
                .setPseudoConfig(dummyPseudoConfig())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path2/dataset2")
                        .setTimestamp(now2.toEpochMilli())
                        .build())
                .setType(Dataset.Type.UNBOUNDED)
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig(dummyPseudoConfig())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path3/dataset31")
                        .setTimestamp(now3.toEpochMilli())
                        .build())
                .setType(Dataset.Type.BOUNDED)
                .setValuation(Dataset.Valuation.SENSITIVE)
                .setState(Dataset.DatasetState.RAW)
                .setPseudoConfig(dummyPseudoConfig())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path3/dataset32")
                        .setTimestamp(now4.toEpochMilli())
                        .build())
                .setType(Dataset.Type.UNBOUNDED)
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.TEMP)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .build());

        String catalogJson = client.get("/catalog").expect200Ok().body();
        JsonNode actual = mapper.readTree(catalogJson);

        ObjectNode expected = mapper.createObjectNode();
        ArrayNode catalogs = expected.putArray("catalogs");
        ObjectNode currentDataset;

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path1/dataset1")
                .put("timestamp", now1.toEpochMilli());
        currentDataset
                .put("type", Dataset.Type.BOUNDED.toString())
                .put("valuation", Dataset.Valuation.OPEN.toString())
                .put("state", Dataset.DatasetState.INPUT.toString());
        currentDataset.putObject("pseudoConfig")
                .put("vars", dummyPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path2/dataset2")
                .put("timestamp", now2.toEpochMilli());
        currentDataset
                .put("type", Dataset.Type.UNBOUNDED.toString())
                .put("valuation", Dataset.Valuation.INTERNAL.toString())
                .put("state", Dataset.DatasetState.PROCESSED.toString());
        currentDataset.putObject("pseudoConfig")
                .put("vars", dummyPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path3/dataset31")
                .put("timestamp", now3.toEpochMilli());
        currentDataset
                .put("type", Dataset.Type.BOUNDED.toString())
                .put("valuation", Dataset.Valuation.SENSITIVE.toString())
                .put("state", Dataset.DatasetState.RAW.toString());
        currentDataset.putObject("pseudoConfig")
                .put("vars", dummyPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path3/dataset32")
                .put("timestamp", now4.toEpochMilli());
        currentDataset
                .put("type", Dataset.Type.UNBOUNDED.toString())
                .put("valuation", Dataset.Valuation.SHIELDED.toString())
                .put("state", Dataset.DatasetState.TEMP.toString());

        assertEquals(expected, actual);
    }

    @Test
    void thatCatalogGetAllDatasetsInPath() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Long timestampInMS = Instant.parse("2018-08-19T16:02:42.00Z").toEpochMilli();

        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path1/dataset1").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path2/dataset2").build()).build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path3/dataset31")
                        .setTimestamp(timestampInMS)
                        .build())
                .setPseudoConfig(dummyPseudoConfig())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path3/dataset32")
                        .setTimestamp(timestampInMS)
                        .build())
                .setPseudoConfig(dummyPseudoConfig())
                .build());
        String catalogJson = client.get("/catalog/path3").expect200Ok().body();
        JsonNode actual = mapper.readTree(catalogJson);

        ObjectNode expected = mapper.createObjectNode();
        ArrayNode catalogs = expected.putArray("catalogs");
        ObjectNode currentDataset;

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path3/dataset31")
                .put("timestamp", timestampInMS);
        currentDataset
                .put("type", Dataset.Type.BOUNDED.toString())
                .put("valuation", Dataset.Valuation.SENSITIVE.toString())
                .put("state", Dataset.DatasetState.OTHER.toString());
        currentDataset.putObject("pseudoConfig")
                .put("vars", dummyPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path3/dataset32")
                .put("timestamp", timestampInMS);
        currentDataset
                .put("type", Dataset.Type.BOUNDED.toString())
                .put("valuation", Dataset.Valuation.SENSITIVE.toString())
                .put("state", Dataset.DatasetState.OTHER.toString());
        currentDataset.putObject("pseudoConfig")
                .put("vars", dummyPseudoConfig().getVarsList().toString());

        assertEquals(expected, actual);
    }

    private DatasetMetaAll createDatasetMetaAll(int i) {
        String path = "/path/to/dataset-" + i;
        return no.ssb.dapla.dataset.api.DatasetMetaAll.newBuilder()
                .setId(no.ssb.dapla.dataset.api.DatasetId.newBuilder().setPath(path).setVersion("" + i).build())
                .setType(no.ssb.dapla.dataset.api.Type.BOUNDED)
                .setValuation(no.ssb.dapla.dataset.api.Valuation.OPEN)
                .setState(no.ssb.dapla.dataset.api.DatasetState.INPUT)
                .setCreatedBy("user")
                .setRandom("rnd-" + i)
                .setParentUri("file://parent/uri")
                .build();
    }

    static no.ssb.dapla.dataset.api.PseudoConfig dummyDatasetMetaAllPseudoConfig() {
        return no.ssb.dapla.dataset.api.PseudoConfig.newBuilder()
                .addVars(no.ssb.dapla.dataset.api.VarPseudoConfigItem.newBuilder().setVar("var1").setPseudoFunc("someFunc1(param1,keyId1)"))
                .addVars(no.ssb.dapla.dataset.api.VarPseudoConfigItem.newBuilder().setVar("var2").setPseudoFunc("someFunc2(keyId2)"))
                .addVars(no.ssb.dapla.dataset.api.VarPseudoConfigItem.newBuilder().setVar("var3").setPseudoFunc("someFunc3(keyId1)"))
                .build();
    }

    static PseudoConfig dummyPseudoConfig() {
        return PseudoConfig.newBuilder()
                .addVars(VarPseudoConfigItem.newBuilder().setVar("var1").setPseudoFunc("someFunc1(param1,keyId1)"))
                .addVars(VarPseudoConfigItem.newBuilder().setVar("var2").setPseudoFunc("someFunc2(keyId2)"))
                .addVars(VarPseudoConfigItem.newBuilder().setVar("var3").setPseudoFunc("someFunc3(keyId1)"))
                .build();
    }

    Dataset repositoryGet(String id) {
        return application.get(DatasetRepository.class).get(id).await(3, TimeUnit.SECONDS);
    }

    GetDatasetResponse get(String path) {
        GetDatasetRequest request = GetDatasetRequest.newBuilder().setPath(path).build();
        return client.postAsJson("/rpc/CatalogService/get", request, GetDatasetResponse.class).expect200Ok().body();
    }

    GetDatasetResponse get(String path, long timestamp) {
        GetDatasetRequest request = GetDatasetRequest.newBuilder().setPath(path).setTimestamp(timestamp).build();
        return client.postAsJson("/rpc/CatalogService/get", request, GetDatasetResponse.class).expect200Ok().body();
    }

    String save(DatasetMetaAll dataset, String userId) {
        byte[] datasetBytes = dataset.toByteArray();
        byte[] signature = metadataSigner.sign(datasetBytes);

        String[] headers = new String[]{"Authorization", "Bearer " + JWT.create().withClaim("preferred_username", userId)
                .sign(Algorithm.HMAC256("secret"))};

        //Authorized user
        SignedDataset signedDataset = SignedDataset.newBuilder()
                .setDatasetMetaAllBytes(ByteString.copyFrom(datasetBytes))
                .setDatasetMetaAllSignatureBytes(ByteString.copyFrom(signature))
                .build();
        return client.postAsJson("/catalog/write", signedDataset, headers).expect200Ok().body();
    }

    DeleteDatasetResponse delete(String path, long timestamp, String username) {
        String[] headers = new String[]{"Authorization", "Bearer " + JWT.create().withClaim("preferred_username", username)
                .sign(Algorithm.HMAC256("secret"))};
        DeleteDatasetRequest request = DeleteDatasetRequest.newBuilder().setPath(path).setTimestamp(timestamp).build();
        return client.postAsJson("/rpc/CatalogService/delete", request, DeleteDatasetResponse.class, headers).expect200Ok().body();
    }

    ListByPrefixResponse listByPrefix(String prefix, int limit) {
        ListByPrefixRequest request = ListByPrefixRequest.newBuilder().setPrefix(prefix).setLimit(limit).build();
        return client.postAsJson("/rpc/CatalogService/listByPrefix", request, ListByPrefixResponse.class).expect200Ok().body();
    }

    @Test
    void thatListByPrefixWorks() {

        // Create datasets, some with multiple versions
        repositoryCreate("/another/prefix");
        repositoryCreate("/unit-test/and/other/data");
        repositoryCreate("/unit-test/and/other/data");
        repositoryCreate("/unit-test/and/other/data");
        repositoryCreate("/unit-test/with/data/1");
        repositoryCreate("/unit-test/with/data/1");
        repositoryCreate("/unit-test/with/data/2");
        repositoryCreate("/unit-test/with/data/3");
        repositoryCreate("/unitisgood/forall");
        repositoryCreate("/x-after/and/more/data");
        ListByPrefixResponse response = listByPrefix("/unit", 100);
        List<DatasetId> entries = response.getEntriesList();
        assertThat(entries.size()).isEqualTo(5);
        assertThat(entries.get(0).getPath()).isEqualTo("/unit-test/and/other/data");
        assertThat(entries.get(1).getPath()).isEqualTo("/unit-test/with/data/1");
        assertThat(entries.get(2).getPath()).isEqualTo("/unit-test/with/data/2");
        assertThat(entries.get(3).getPath()).isEqualTo("/unit-test/with/data/3");
        assertThat(entries.get(4).getPath()).isEqualTo("/unitisgood/forall");
    }

    @Test
    void thatGetFoldersWorks() {
        var emptyResponse = client.get("/folder?prefix=/&version=2018-08-19T16:02:42.00Z");

        // Assert empty.
        assertThat(emptyResponse.body()).isEqualToIgnoringWhitespace("""
           {} 
        """);

        repositoryCreate("/foo/dataset1", Instant.ofEpochMilli(10000));
        repositoryCreate("/foo/bar1/dataset1", Instant.ofEpochMilli(20000));
        repositoryCreate("/foo/bar2/dataset1", Instant.ofEpochMilli(40000));
        repositoryCreate("/bar/foo1/dataset1", Instant.ofEpochMilli(30000));
        repositoryCreate("/bar/foo2/dataset1", Instant.ofEpochMilli(50000));

        var rootResponse = client.get("/folder?prefix=/");
        assertThat(rootResponse.body()).isEqualToIgnoringWhitespace("""
                {
                  "entries": [{
                    "path": "bar",
                    "timestamp": "50000"
                  }, {
                    "path": "foo",
                    "timestamp": "40000"
                  }]
                }
                """);

        // Note how the folder inherits the date of the latest child.
        var pastRootResponse = client.get("/folder?prefix=/&version=1970-01-01T00:00:39Z");
        assertThat(pastRootResponse.body()).isEqualToIgnoringWhitespace("""
                {
                  "entries": [{
                    "path": "bar",
                    "timestamp": "30000"
                  }, {
                    "path": "foo",
                    "timestamp": "20000"
                  }]
                }
                """);

        var fooResponse = client.get("/folder?prefix=/foo");
        assertThat(fooResponse.body()).isEqualToIgnoringWhitespace("""
                {
                  "entries": [{
                    "path": "/foo/bar1",
                    "timestamp": "20000"
                  }, {
                    "path": "/foo/bar2",
                    "timestamp": "40000"
                  }]
                }
                """);

        var barResponse = client.get("/folder?prefix=/bar");
        assertThat(barResponse.body()).isEqualToIgnoringWhitespace("""
                {
                  "entries": [{
                    "path": "/bar/foo1",
                    "timestamp": "30000"
                  }, {
                    "path": "/bar/foo2",
                    "timestamp": "50000"
                  }]
                }
                """);

    }

    @Test
    void thatGetDatasetWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.OUTPUT)
                .setPseudoConfig(dummyPseudoConfig())
                .setParentUri("f1")
                .build();
        repositoryCreate(dataset);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setPath("2").build())
                        .setValuation(Dataset.Valuation.SENSITIVE)
                        .setState(Dataset.DatasetState.RAW)
                        .setPseudoConfig(dummyPseudoConfig())
                        .setParentUri("file")
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
                .setId(DatasetId.newBuilder().setPath("a_dataset").build())
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig(dummyPseudoConfig())
                .build();
        repositoryCreate(old);

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setPath("a_dataset").build())
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .setParentUri("a_location")
                        .build()
        );

        assertThat(get("a_dataset", timestamp).getDataset()).isEqualTo(old);
    }

    @Test
    void thatGetPreviousReturnsNothingWhenTimestampIsOld() {
        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setPath("dataset_from_after_timestamp").build())
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .setParentUri("a_location")
                        .build()
        );
        assertThat(get("dataset_from_after_timestamp", 100L).hasDataset()).isFalse();
    }

    @Test
    void thatGetPreviousReturnsTheLatestDatasetWhenTimestampIsAfterTheLatest() {
        long now = System.currentTimeMillis();
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("dataset_from_before_timestamp").setTimestamp(now).build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.PRODUCT)
                .setPseudoConfig(dummyPseudoConfig())
                .setParentUri("some_file")
                .build();
        repositoryCreate(dataset);

        assertThat(get("dataset_from_before_timestamp", now + 1).getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatDeleteWorks() {
        long now = System.currentTimeMillis();
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("dataset_to_delete")
                        .setTimestamp(now)
                        .build())
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.RAW)
                .setParentUri("f")
                .build();
        repositoryCreate(dataset);
        delete(dataset.getId().getPath(), now, "a-user");
        assertThat(repositoryGet("dataset_to_delete")).isNull();
    }

    @Test
    void thatDeleteWorksWhenDatasetDoesntExist() {
        delete("does_not_exist", 0, "a-user");
    }

    @Test
    void thatCreateWorksIfUserHasCreateAccess() {

        String version = "" + System.currentTimeMillis();

        no.ssb.dapla.dataset.api.DatasetMetaAll ds1 = no.ssb.dapla.dataset.api.DatasetMetaAll.newBuilder()
                .setId(no.ssb.dapla.dataset.api.DatasetId.newBuilder()
                        .setPath("dataset_to_create")
                        .setVersion(version)
                        .build())
                .setType(no.ssb.dapla.dataset.api.Type.BOUNDED)
                .setValuation(no.ssb.dapla.dataset.api.Valuation.SENSITIVE)
                .setState(no.ssb.dapla.dataset.api.DatasetState.OUTPUT)
                .setPseudoConfig(dummyDatasetMetaAllPseudoConfig())
                .setCreatedBy("a-user")
                .setRandom("ds1")
                .setParentUri("file_location")
                .build();

        no.ssb.dapla.dataset.api.DatasetMetaAll ds2 = no.ssb.dapla.dataset.api.DatasetMetaAll.newBuilder()
                .setId(no.ssb.dapla.dataset.api.DatasetId.newBuilder()
                        .setPath("dataset_to_create")
                        .setVersion(version)
                        .build())
                .setType(no.ssb.dapla.dataset.api.Type.BOUNDED)
                .setValuation(no.ssb.dapla.dataset.api.Valuation.INTERNAL)
                .setState(no.ssb.dapla.dataset.api.DatasetState.PROCESSED)
                .setPseudoConfig(dummyDatasetMetaAllPseudoConfig())
                .setCreatedBy("a-user")
                .setRandom("ds2")
                .setParentUri("file_location_2")
                .build();

        save(ds1, "a-user");

        assertThat(repositoryGet("dataset_to_create").getValuation()).isEqualTo(Dataset.Valuation.SENSITIVE);

        save(ds2, "a-user");

        assertThat(repositoryGet("dataset_to_create").getValuation()).isEqualTo(Dataset.Valuation.INTERNAL);
    }

    @Test
    void thatCreateFailsIfUserHasNoCreateAccess() {
        String version = "" + System.currentTimeMillis();
        no.ssb.dapla.dataset.api.DatasetMetaAll ds1 = no.ssb.dapla.dataset.api.DatasetMetaAll.newBuilder()
                .setId(no.ssb.dapla.dataset.api.DatasetId.newBuilder()
                        .setPath("dataset_to_create")
                        .setVersion(version)
                        .build())
                .setType(no.ssb.dapla.dataset.api.Type.BOUNDED)
                .setValuation(no.ssb.dapla.dataset.api.Valuation.SENSITIVE)
                .setState(no.ssb.dapla.dataset.api.DatasetState.OUTPUT)
                .setPseudoConfig(dummyDatasetMetaAllPseudoConfig())
                .setCreatedBy("b-user")
                .setRandom("ds1")
                .setParentUri("file_location")
                .build();

        save(ds1, "b-user");
    }
}
