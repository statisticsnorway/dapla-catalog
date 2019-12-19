package no.ssb.dapla.catalog.repository;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.util.logging.LogManager;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetRepositoryTest {

    static {
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private static BigtableDataClient dataClient;

    private static BigtableTableAdminClient adminClient;

    @BeforeAll
    public static void beforeAll() throws Exception {
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings
                .newBuilderForEmulator("localhost", 9035)
                .setProjectId("my-project")
                .setInstanceId("my-instance")
                .build();

        adminClient = BigtableTableAdminClient.create(adminSettings);

        if (!adminClient.exists("dataset")) {
            CreateTableRequest createTableRequest = CreateTableRequest.of("dataset").addFamily("document");
            adminClient.createTable(createTableRequest);
        }

        BigtableDataSettings dataSettings = BigtableDataSettings
                .newBuilderForEmulator("localhost", 9035)
                .setProjectId("my-project")
                .setInstanceId("my-instance")
                .build();

        dataClient = BigtableDataClient.create(dataSettings);
    }

    @AfterAll
    public static void afterAll() {
        dataClient.close();
        adminClient.close();
    }

    @AfterEach
    public void afterEach() {
        adminClient.dropAllRows("dataset");
    }

    @Test
    void thatDeleteWorks() throws Exception {
        DatasetRepository repository = new DatasetRepository(dataClient);

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
        assertThat(repository.get("to_be_deleted")).isNull();
        assertThat(repository.get("should_not_be_deleted")).isNotNull();
    }

    @Test
    void thatGetMostRecentAtAGivenTimeWorks() throws Exception {
        DatasetRepository repository = new DatasetRepository(dataClient);

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

        Dataset dataset = repository.get("1", timestamp);
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

        DatasetRepository repository = new DatasetRepository(dataClient);
        repository.create(ds1);
        repository.create(ds2);
        repository.create(ds3);

        Dataset dataset = repository.get("1");

        assertThat(dataset.getId()).isEqualTo("1");
        assertThat(dataset.getState()).isEqualTo(DatasetState.INPUT);
        assertThat(dataset.getValuation()).isEqualTo(Valuation.INTERNAL);
        assertThat(dataset.getLocationsList().asByteStringList()).containsExactly(ByteString.copyFrom("gcs://another-file".getBytes()));
    }
}
