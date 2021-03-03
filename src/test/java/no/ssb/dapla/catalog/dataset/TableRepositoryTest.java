package no.ssb.dapla.catalog.dataset;

import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.CatalogTable;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(IntegrationTestExtension.class)
public class TableRepositoryTest {

    @Inject
    CatalogApplication application;

    TableRepository repository;

    @BeforeEach
    public void beforeEach() {
        application.get(TableRepository.class).deleteAll().await(30, TimeUnit.SECONDS);
        repository = application.get(TableRepository.class);
    }

    @Test
    void thatCreateWorks() {
        CatalogTable catalogTable = CatalogTable.newBuilder().setPath("/my/dataset").setMetadataLocation("loc1").build();
        repository.create(catalogTable).await();
        assertThat(repository.get("/my/dataset").await()).isEqualTo(catalogTable);

    }
    @Test
    void thatUpdateWorks() {
        CatalogTable catalogTable = CatalogTable.newBuilder().setPath("/my/dataset").setMetadataLocation("loc1").build();
        repository.create(catalogTable).await();
        CatalogTable updatedTable = CatalogTable.newBuilder(catalogTable).setMetadataLocation("loc2").build();
        repository.update(updatedTable, catalogTable.getMetadataLocation()).await();
        assertThat(repository.get("/my/dataset").await()).isEqualTo(updatedTable);
    }
}
