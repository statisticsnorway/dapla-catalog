package no.ssb.dapla.catalog.dataset;

import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.MockRegistryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(IntegrationTestExtension.class)
@MockRegistryConfig(CatalogMockRegistry.class)
class DatasetRepositoryTest {

    @Inject
    CatalogApplication application;

    @BeforeEach
    public void beforeEach() {
        application.get(DatasetRepository.class).deleteAllDatasets().await(30, TimeUnit.SECONDS);
    }

    Dataset createDataset(String path, Instant time) {
        return Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath(path)
                        .setTimestamp(time.toEpochMilli()
                        ).build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri(path)
                .build();
    }

    @Test
    void listingDatasetByPrefix() {
        DatasetRepository repository = application.get(DatasetRepository.class);

        var now = Instant.now();
        repository.create(createDataset("/a/a/1", now)).await();
        repository.create(createDataset("/a/1", now.minus(1, ChronoUnit.DAYS))).await();
        repository.create(createDataset("/a/2", now)).await();
        repository.create(createDataset("/a/1", now)).await();
        repository.create(createDataset("/a/b/1", now)).await();
        repository.create(createDataset("/a/b/2", now)).await();
        repository.create(createDataset("/a/3", now)).await();

        var foldersNoLimit = repository.listDatasetsByPrefix(
                "/a", ZonedDateTime.now(), Integer.MAX_VALUE)
                .collectList().await();
        assertThat(foldersNoLimit).extracting(Dataset::getId).extracting(DatasetId::getPath)
                .containsExactly(
                        "/a/1",
                        "/a/2",
                        "/a/3"
                );
    }

    @Test
    void listingFoldersByPrefix() {
        DatasetRepository repository = application.get(DatasetRepository.class);

        var now = Instant.now();
        repository.create(createDataset("/a/a/1", now)).await();
        repository.create(createDataset("/a/a/1", now.minus(1, ChronoUnit.DAYS))).await();
        repository.create(createDataset("/a/a/2", now)).await();
        repository.create(createDataset("/a/a/3", now)).await();
        repository.create(createDataset("/a/b/1", now)).await();
        repository.create(createDataset("/a/b/2", now)).await();
        repository.create(createDataset("/a/c", now)).await();
        repository.create(createDataset("/a/d/@/alpha", now)).await();
        repository.create(createDataset("/a/d/β/beta", now)).await();

        var foldersNoLimit = repository.listFoldersByPrefix(
                "/a", ZonedDateTime.now(), Integer.MAX_VALUE)
                .collectList().await();
        assertThat(foldersNoLimit).extracting(DatasetId::getPath)
                .containsExactly(
                        "/a/a",
                        "/a/b",
                        "/a/d"
                );
    }

    @Test
    void thatDeleteWorks() {
        DatasetRepository repository = application.get(DatasetRepository.class);

        long now = System.currentTimeMillis();

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("to_be_deleted")
                        .setTimestamp(now - 100)
                        .build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("f1")
                .build();
        repository.create(ds1).await();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("to_be_deleted")
                        .setTimestamp(now - 50)
                        .build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.OPEN)
                .setParentUri("f1")
                .build();
        repository.create(ds2).await();

        Dataset ds3 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("should_not_be_deleted")
                        .setTimestamp(now - 10)
                        .build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.SENSITIVE)
                .setParentUri("f1")
                .build();
        repository.create(ds3).await();

        repository.delete("to_be_deleted", now - 100).await();
        repository.delete("to_be_deleted", now - 50).await();
        assertThat(repository.get("to_be_deleted").await()).isNull();
        assertThat(repository.get("should_not_be_deleted").await()).isNotNull();
    }

    @Test
    void thatGetMostRecentAtAGivenTimeWorks() throws InterruptedException {
        DatasetRepository repository = application.get(DatasetRepository.class);

        var time = Instant.now();

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("1")
                        .setTimestamp(time.minusSeconds(1000).toEpochMilli()).build())
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .setParentUri("gcs://some-file")
                .build();
        repository.create(ds1).await();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1")
                        .setTimestamp(time.toEpochMilli()).build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("gcs://another-file")
                .build();
        repository.create(ds2).await();

        assertThat(repository.get("1", time.minusSeconds(500).toEpochMilli()).await()).isEqualTo(ds1);
    }

    @Test
    void thatGetAnExactVersionWorks() throws InterruptedException {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").setTimestamp(10).build())
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .setParentUri("gcs://some-file")
                .build();
        repository.create(ds1).await();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").setTimestamp(20).build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("gcs://another-file")
                .build();
        repository.create(ds2).await();

        assertThat(repository.get("1", 10).await()).isEqualTo(ds1);
    }

    @Test
    void thatWriteWorks() {

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .setParentUri("gcs://some-file")
                .build();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("gcs://another-file")
                .build();

        Dataset ds3 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("2").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("gcs://a-file")
                .build();

        DatasetRepository repository = application.get(DatasetRepository.class);
        repository.create(ds1).await();
        repository.create(ds2).await();
        repository.create(ds3).await();

        assertThat(repository.get("1").await()).isEqualTo(ds2);
    }
}
