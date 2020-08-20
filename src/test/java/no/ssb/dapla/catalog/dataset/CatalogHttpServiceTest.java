package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(IntegrationTestExtension.class)
class CatalogHttpServiceTest {

    @Inject
    CatalogApplication application;

    @Inject
    TestClient client;

    @Inject
    AuthServiceGrpc.AuthServiceFutureStub authServiceFutureStub;


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

        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path1/dataset1").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path2/dataset2").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset31").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset32").build()).build());

        String catalogJson = client.get("/catalog").expect200Ok().body();
        JsonNode actual = mapper.readTree(catalogJson);

        ObjectNode expected = mapper.createObjectNode();
        ArrayNode catalogs = expected.putArray("catalogs");
        catalogs.addObject().putObject("id").put("path", "/path1/dataset1");
        catalogs.addObject().putObject("id").put("path", "/path2/dataset2");
        catalogs.addObject().putObject("id").put("path", "/path3/dataset31");
        catalogs.addObject().putObject("id").put("path", "/path3/dataset32");

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
        CatalogSigner metadataSigner = new CatalogSigner("PKCS12", "src/test/resources/metadata-signer_keystore.p12",
                "dataAccessKeyPair", "changeit".toCharArray(), "SHA256withRSA");
        byte[] signature = metadataSigner.sign(validMetadataJson.toByteArray());
        writeContentToFile(dataFolder, datasetMeta, ".dataset-meta.json.sign", signature);
        String metadataSignaturePath = datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json.sign";

        Dataset dataset = Dataset.newBuilder().build();
        SignedDataset signedDataset = SignedDataset.newBuilder()
                .setDataset(dataset)
                .setUserId("user")
                .build();
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path1").build()).build());
        client.post("/catalog/save", signedDataset).expect200Ok();
    }


}
