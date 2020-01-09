package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(IntegrationTestExtension.class)
class DatasetRepositoryTest {

    @Inject
    Application application;

    @BeforeEach
    public void beforeEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows(DatasetRepository.TABLE_ID);
        application.get(BigtableTableAdminClient.class).dropAllRows(NameIndex.TABLE_ID);
    }

    @Test
    void thatDeleteWorks() {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("to_be_deleted").build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("f1")
                .addLocations("f2")
                .build();
        repository.create(ds1).join();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("to_be_deleted").build())
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.OPEN)
                .addLocations("f1")
                .addLocations("f2")
                .addLocations("f3")
                .build();
        repository.create(ds2).join();

        Dataset ds3 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("should_not_be_deleted").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.SENSITIVE)
                .addLocations("f1")
                .build();
        repository.create(ds3).join();

        repository.delete("to_be_deleted").join();
        assertThat(repository.get("to_be_deleted").join()).isNull();
        assertThat(repository.get("should_not_be_deleted").join()).isNotNull();
    }

    @Test
    void thatGetMostRecentAtAGivenTimeWorks() throws InterruptedException {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("1").build())
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .addLocations("gcs://some-file")
                .build();
        repository.create(ds1).join();

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("1").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("gcs://another-file")
                .build();
        repository.create(ds2).join();

        assertThat(repository.get("1", timestamp).join()).isEqualTo(ds1);
    }

    @Test
    void thatWriteWorks() {

        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("1").build())
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .addLocations("gcs://some-file")
                .build();

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("1").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("gcs://another-file")
                .build();

        Dataset ds3 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("2").build())
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("gcs://a-file")
                .build();

        DatasetRepository repository = application.get(DatasetRepository.class);
        repository.create(ds1).join();
        repository.create(ds2).join();
        repository.create(ds3).join();

        assertThat(repository.get("1").join()).isEqualTo(ds2);
    }
}
