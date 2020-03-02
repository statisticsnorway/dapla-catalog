package no.ssb.dapla.catalog.dataset;

import no.ssb.dapla.catalog.CatalogApplication;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(IntegrationTestExtension.class)
class DatasetRepositoryTest {

    @Inject
    CatalogApplication application;

    @BeforeEach
    public void beforeEach() {
        application.get(DatasetRepository.class).deleteAllDatasets().blockingGet();
    }

    @Test
    void thatDeleteWorks() {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("to_be_deleted").build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("f1")
                .build();
        repository.create(ds1).blockingGet();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("to_be_deleted").build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.OPEN)
                .setParentUri("f1")
                .build();
        repository.create(ds2).blockingGet();

        Dataset ds3 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("should_not_be_deleted").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.SENSITIVE)
                .setParentUri("f1")
                .build();
        repository.create(ds3).blockingGet();

        repository.delete("to_be_deleted").blockingGet();
        assertThat(repository.get("to_be_deleted").blockingGet()).isNull();
        assertThat(repository.get("should_not_be_deleted").blockingGet()).isNotNull();
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
        repository.create(ds1).blockingGet();

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .setParentUri("gcs://another-file")
                .build();
        repository.create(ds2).blockingGet();

        assertThat(repository.get("1", timestamp).blockingGet()).isEqualTo(ds1);
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
        repository.create(ds1).blockingGet();
        repository.create(ds2).blockingGet();
        repository.create(ds3).blockingGet();

        assertThat(repository.get("1").blockingGet()).isEqualTo(ds2);
    }
}
