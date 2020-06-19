package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(IntegrationTestExtension.class)
class CatalogHttpServiceTest {

    @Inject
    CatalogApplication application;

    @Inject
    TestClient client;

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
}
