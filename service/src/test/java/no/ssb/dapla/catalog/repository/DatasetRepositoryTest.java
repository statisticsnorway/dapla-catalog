package no.ssb.dapla.catalog.repository;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class DatasetRepositoryTest {

    @Inject
    Application application;

    @AfterEach
    public void afterEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows("dataset");
    }

    @Test
    void thatDeleteWorks() throws Exception {
        DatasetRepository repository = application.get(DatasetRepository.class);

        Dataset ds1 = Dataset.newBuilder()
                .setId("to_be_deleted")
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("f1")
                .addLocations("f2")
                .build();
        repository.create(ds1);

        Dataset ds2 = Dataset.newBuilder()
                .setId("to_be_deleted")
                .setState(DatasetState.PRODUCT)
                .setValuation(Valuation.OPEN)
                .addLocations("f1")
                .addLocations("f2")
                .addLocations("f3")
                .build();
        repository.create(ds2);

        Dataset ds3 = Dataset.newBuilder()
                .setId("should_not_be_deleted")
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.SENSITIVE)
                .addLocations("f1")
                .build();
        repository.create(ds3);

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
        repository.create(ds1);

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        Dataset ds2 = Dataset.newBuilder()
                .setId("1")
                .setState(DatasetState.INPUT)
                .setValuation(Valuation.INTERNAL)
                .addLocations("gcs://another-file")
                .build();

        repository.create(ds2);

        Dataset dataset = repository.get("1", timestamp).join();
        assertThat(dataset.getId()).isEqualTo("1");
        assertThat(dataset.getState()).isEqualTo(DatasetState.RAW);
        assertThat(dataset.getValuation()).isEqualTo(Valuation.SHIELDED);
        assertThat(dataset.getLocationsList().asByteStringList()).containsExactly(ByteString.copyFrom("gcs://some-file".getBytes()));
    }

    @Test
    void thatWriteWorks() throws IOException {

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
        repository.create(ds1);
        repository.create(ds2);
        repository.create(ds3);

        Dataset dataset = repository.get("1").join();

        assertThat(dataset.getId()).isEqualTo("1");
        assertThat(dataset.getState()).isEqualTo(DatasetState.INPUT);
        assertThat(dataset.getValuation()).isEqualTo(Valuation.INTERNAL);
        assertThat(dataset.getLocationsList().asByteStringList()).containsExactly(ByteString.copyFrom("gcs://another-file".getBytes()));
    }
}
