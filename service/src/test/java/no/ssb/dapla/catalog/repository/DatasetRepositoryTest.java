package no.ssb.dapla.catalog.repository;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static no.ssb.dapla.catalog.DatasetAssert.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class DatasetRepositoryTest {

    @Inject
    Application application;

    @AfterEach
    public void afterEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows("dataset");
    }

    @Test
    void thatDeleteWorks() {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId("to_be_deleted")
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("f1")
                .addLocations("f2")
                .build();
        repository.create(ds1).join();

        Dataset ds2 = Dataset.newBuilder()
                .setId("to_be_deleted")
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.OPEN)
                .addLocations("f1")
                .addLocations("f2")
                .addLocations("f3")
                .build();
        repository.create(ds2).join();

        Dataset ds3 = Dataset.newBuilder()
                .setId("should_not_be_deleted")
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.SENSITIVE)
                .addLocations("f1")
                .build();
        repository.create(ds3).join();

        repository.delete("to_be_deleted");
        assertThat(repository.get("to_be_deleted").join()).isNull();
        assertThat(repository.get("should_not_be_deleted").join()).isNotNull();
    }

    @Test
    void thatGetMostRecentAtAGivenTimeWorks() throws Exception {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId("1")
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .addLocations("gcs://some-file")
                .build();
        repository.create(ds1).join();

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        Dataset ds2 = Dataset.newBuilder()
                .setId("1")
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
                .setId("1")
                .setState(DatasetState.RAW)
                .setValuation(Valuation.SHIELDED)
                .addLocations("gcs://some-file")
                .build();

        Dataset ds2 = Dataset.newBuilder()
                .setId("1")
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("gcs://another-file")
                .build();

        Dataset ds3 = Dataset.newBuilder()
                .setId("2")
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
