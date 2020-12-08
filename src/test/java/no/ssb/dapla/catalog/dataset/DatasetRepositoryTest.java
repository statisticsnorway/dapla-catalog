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

    @Test
    void listingByPrefixAndDepth() {
        // TODO:
        //  Add some data.
        //  Make sure to use weird chars.
        //  Query with and without depth.
        //  Query with and without limit.
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

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .setParentUri("gcs://some-file")
                .build();
        repository.create(ds1).await();

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("gcs://another-file")
                .build();
        repository.create(ds2).await();

        assertThat(repository.get("1", timestamp).await()).isEqualTo(ds1);
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
