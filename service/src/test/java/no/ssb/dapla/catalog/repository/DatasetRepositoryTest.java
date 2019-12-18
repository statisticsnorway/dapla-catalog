package no.ssb.dapla.catalog.repository;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.protobuf.Dataset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    void createTableIfNotExists(String tableId, String columnFamily) {

        BigtableTableAdminSettings settings;
        try {
            settings = BigtableTableAdminSettings
                    .newBuilderForEmulator("localhost", 9035)
                    .setProjectId("my-project")
                    .setInstanceId("my-instance")
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        try (BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(settings)) {

            if (!adminClient.exists(tableId)) {
                CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(columnFamily);
                adminClient.createTable(createTableRequest);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void thatWriteWorks() throws IOException {

        createTableIfNotExists("dataset", "document");

        BigtableDataSettings settings = BigtableDataSettings
                .newBuilderForEmulator("localhost", 9035)
                .setProjectId("my-project")
                .setInstanceId("my-instance")
                .build();

        Dataset ds1 = Dataset.newBuilder()
                .setId("0162a6e9-a2c7-4079-82f5-f14b484b0fa9")
                .setState(Dataset.DatasetState.RAW)
                .setValuation(Dataset.Valuation.SHIELDED)
                .addLocations("gcs://some-file")
                .build();

        Dataset ds2 = Dataset.newBuilder()
                .setId("0162a6e9-a2c7-4079-82f5-f14b484b0fa9")
                .setState(Dataset.DatasetState.INPUT)
                .setValuation(Dataset.Valuation.INTERNAL)
                .addLocations("gcs://another-file")
                .build();

        Dataset ds3 = Dataset.newBuilder()
                .setId("a5c84237-dbea-4ec0-acb0-03c732bf8040")
                .setState(Dataset.DatasetState.INPUT)
                .setValuation(Dataset.Valuation.INTERNAL)
                .addLocations("gcs://another-file")
                .build();

        try (BigtableDataClient client = BigtableDataClient.create(settings)) {

            DatasetRepository repository = new DatasetRepository(client);
            repository.create(ds1);
            repository.create(ds2);
            repository.create(ds3);

            Dataset dataset = repository.get("0162a6e9-a2c7-4079-82f5-f14b484b0fa9");

            assertThat(dataset.getId()).isEqualTo("0162a6e9-a2c7-4079-82f5-f14b484b0fa9");
            assertThat(dataset.getState()).isEqualTo(Dataset.DatasetState.INPUT);
            assertThat(dataset.getValuation()).isEqualTo(Dataset.Valuation.INTERNAL);
            assertThat(dataset.getLocationsList().asByteStringList()).containsExactly(ByteString.copyFrom("gcs://another-file".getBytes()));
        }
    }
}
