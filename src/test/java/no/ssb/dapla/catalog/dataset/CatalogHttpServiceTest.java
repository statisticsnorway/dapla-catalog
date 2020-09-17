package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(IntegrationTestExtension.class)
class CatalogHttpServiceTest {

    @Inject
    CatalogApplication application;

    @Inject
    TestClient client;

    String char256 = "fake_signature_of_256_lengthdapla_testing_fake_sinagure of 228 characters length is a long string of chars that is used for testing a fake signature of not importance, ... now it is already 165 chars and i only need to create another 10s of chars to fill .";

    @BeforeEach
    public void beforeEach() {
        application.get(DatasetRepository.class).deleteAllDatasets().blockingGet();
    }

    void repositoryCreate(Dataset dataset) {
        application.get(DatasetRepository.class).create(dataset).timeout(3, TimeUnit.SECONDS).blockingGet();
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
    void thatCatalogGetAllDatasets() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Clock clockDS1 = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        Clock clockDS2 = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        Clock clockDS3 = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        Clock clockDS4 = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path1/dataset1")
                        .setTimestamp(clockDS1.millis())
                        .build())
                .setType(Dataset.Type.BOUNDED)
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.INPUT)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path2/dataset2")
                        .setTimestamp(clockDS2.millis())
                        .build())
                .setType(Dataset.Type.UNBOUNDED)
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path3/dataset31")
                        .setTimestamp(clockDS3.millis())
                        .build())
                .setType(Dataset.Type.BOUNDED)
                .setValuation(Dataset.Valuation.SENSITIVE)
                .setState(Dataset.DatasetState.RAW)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .build());
        repositoryCreate(Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/path3/dataset32")
                        .setTimestamp(clockDS4.millis())
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
                .put("timestamp", clockDS1.millis());
        currentDataset
                .put("type", Dataset.Type.BOUNDED.getNumber())
                .put("valuation",Dataset.Valuation.OPEN.getNumber())
                .put("state",Dataset.DatasetState.INPUT.getNumber())
                .put("pseudoConfig", Dataset.getDefaultInstance().getPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path2/dataset2")
                .put("timestamp", clockDS2.millis());
        currentDataset
                .put("type", Dataset.Type.UNBOUNDED.getNumber())
                .put("valuation",Dataset.Valuation.INTERNAL.getNumber())
                .put("state",Dataset.DatasetState.PROCESSED.getNumber())
                .put("pseudoConfig", Dataset.getDefaultInstance().getPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path3/dataset31")
                .put("timestamp", clockDS3.millis());
        currentDataset
                .put("type", Dataset.Type.BOUNDED.getNumber())
                .put("valuation",Dataset.Valuation.SENSITIVE.getNumber())
                .put("state",Dataset.DatasetState.RAW.getNumber())
                .put("pseudoConfig", Dataset.getDefaultInstance().getPseudoConfig().getVarsList().toString());

        currentDataset = catalogs.addObject();
        currentDataset.putObject("id")
                .put("path", "/path3/dataset32")
                .put("timestamp", clockDS4.millis());
        currentDataset
                .put("type", Dataset.Type.UNBOUNDED.getNumber())
                .put("valuation",Dataset.Valuation.SHIELDED.getNumber())
                .put("state",Dataset.DatasetState.TEMP.getNumber())
                .put("pseudoConfig", Dataset.getDefaultInstance().getPseudoConfig().getVarsList().toString());

        assertEquals(expected, actual);
    }

    @Test
    void thatCatalogGetAllDatasetsInPath() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path1/dataset1").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path2/dataset2").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset31").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset32").build()).build());
        String catalogJson = client.get("/catalog/path3").expect200Ok().body();
        JsonNode actual = mapper.readTree(catalogJson);

        ObjectNode expected = mapper.createObjectNode();
        ArrayNode catalogs = expected.putArray("catalogs");
        catalogs.addObject().putObject("id").put("path", "/path3/dataset31");
        catalogs.addObject().putObject("id").put("path", "/path3/dataset32");

        assertEquals(expected, actual);
    }

    @Test
    void thatCatalogSaveDataset() {
        MetadataSigner metadataSigner = new MetadataSigner("PKCS12", "src/test/resources/metadata-signer_keystore.p12",
                "dataAccessKeyPair", "changeit".toCharArray(), "SHA256withRSA");

        Dataset dataset = createDataset(0);
        byte[] signature = metadataSigner.sign(dataset.toByteArray());
        byte[] datasetMetaBytes = dataset.toByteArray();

        //Authorized user
        SignedDataset signedDataset = SignedDataset.newBuilder()
                .setDataset(dataset)
                .setUserId("user")
                .setDatasetMetaBytes(ByteString.copyFrom(datasetMetaBytes))
                .setDatasetMetaSignatureBytes(ByteString.copyFrom(signature))
                .build();
        client.post("/catalog/write", signedDataset).expect200Ok();

        // fake signature
        SignedDataset signedDataset2 = SignedDataset.newBuilder()
                .setDataset(dataset)
                .setUserId("user")
                .setDatasetMetaBytes(ByteString.copyFrom(datasetMetaBytes))
                .setDatasetMetaSignatureBytes(ByteString.copyFrom(char256.getBytes()))
                .build();
        Assertions.assertEquals(401, client.post("/catalog/write", signedDataset2).response().statusCode());

        // missing metadata and signature
        SignedDataset signedDataset3 = SignedDataset.newBuilder()
                .setDataset(dataset)
                .setUserId("user")
                .build();
        Assertions.assertEquals(400, client.post("/catalog/write", signedDataset3).response().statusCode());

    }

    private Dataset createDataset(int i) {
        String path = "/path/to/dataset-" + i;
        return Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath(path).build())
                .setType(Dataset.Type.BOUNDED)
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.INPUT)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .build();
    }
}
