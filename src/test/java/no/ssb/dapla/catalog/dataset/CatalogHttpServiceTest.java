package no.ssb.dapla.catalog.dataset;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.helidon.webserver.ServerResponse;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.dapla.catalog.protobuf.VarPseudoConfigItem;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.testing.helidon.GrpcMockRegistry;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import javax.xml.catalog.Catalog;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;


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
    void thatCatalogGetEmptyList() {
        String catalogJson = client.get("/catalog").expect200Ok().body();
        assertEquals("{ \"catalogs\": ]}", catalogJson);
    }

    @Test
    void thatCatalogGetAllDatasets() {
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path1/dataset1").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path2/dataset2").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset3").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset4").build()).build());
        String catalogJson = client.get("/catalog").expect200Ok().body();
        assertNotEquals("{ \"catalogs\": ]}", catalogJson);
        assertTrue(catalogJson.contains("/path1/dataset1"));
        assertTrue(catalogJson.contains("/path2/dataset2"));
        assertTrue(catalogJson.contains("/path3/dataset3"));
        assertTrue(catalogJson.contains("/path3/dataset4"));
    }

    @Test
    void thatCatalogGetAllDatasetsInPath() {
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path1/dataset1").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path2/dataset2").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset3").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/path3/dataset4").build()).build());
        String catalogJson = client.get("/catalog/path3").expect200Ok().body();
        assertNotEquals("{ \"catalogs\": ]}", catalogJson);
        assertFalse(catalogJson.contains("/path1/dataset1"));
        assertFalse(catalogJson.contains("/path2/dataset2"));
        assertTrue(catalogJson.contains("/path3/dataset3"));
        assertTrue(catalogJson.contains("/path3/dataset4"));
    }
}
