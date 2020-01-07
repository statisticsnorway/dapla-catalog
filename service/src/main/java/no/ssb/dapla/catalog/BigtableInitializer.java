package no.ssb.dapla.catalog;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.helidon.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

public class BigtableInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableInitializer.class);

    static void createBigtableSchemaIfNotExists(Config bigtableConfig, BigtableTableAdminClient adminClient) {
        if (!bigtableConfig.get("generate-schema").asBoolean().orElse(false)) {
            return;
        }
        LOG.info("Initializing Bigtable schema");
        String tableId = bigtableConfig.get("table-id").asString().orElse("dataset");
        if (!adminClient.exists(tableId)) {
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(bigtableConfig.get("column-family").asString().orElse("document"));
            adminClient.createTable(createTableRequest);
        }
    }

    static BigtableTableAdminClient createBigtableAdminClient(Config bigtableConfig) {
        String projectId = bigtableConfig.get("project-id").asString().orElse("my-project");
        String instanceId = bigtableConfig.get("instance-id").asString().orElse("my-instance");
        if (bigtableConfig.get("emulator").asBoolean().orElse(true)) {
            String host = bigtableConfig.get("host").asString().orElse("localhost");
            int port = bigtableConfig.get("port").asInt().orElse(9035);
            LOG.info("Creating Bigtable emulator admin-client");
            return createEmulatorBigtableTableAdminClient(host, port, projectId, instanceId);
        } else {
            Path serviceAccountKeyFilePath = Path.of(bigtableConfig.get("service-account.path").asString()
                    .orElseThrow(() -> new RuntimeException("'service-account.path' missing"))
            );
            LOG.info("Creating Bigtable admin-client");
            return createRealBigtableTableAdminClient(serviceAccountKeyFilePath, projectId, instanceId);
        }
    }

    static BigtableTableAdminClient createEmulatorBigtableTableAdminClient(String host, int port, String projectId, String instanceId) {
        try {
            BigtableTableAdminSettings settings = BigtableTableAdminSettings
                    .newBuilderForEmulator(host, port)
                    .setProjectId(projectId)
                    .setInstanceId(instanceId)
                    .build();
            return BigtableTableAdminClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Failed to connect to bigtable", e);
        }
    }

    static BigtableTableAdminClient createRealBigtableTableAdminClient(Path serviceAccountKeyFilePath, String projectId, String instanceId) {
        try {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(
                    Files.newInputStream(serviceAccountKeyFilePath, StandardOpenOption.READ))
                    .createScoped(Collections.singletonList("https://www.googleapis.com/auth/bigtable.admin.table"));
            BigtableTableAdminSettings settings = BigtableTableAdminSettings
                    .newBuilder()
                    .setProjectId(projectId)
                    .setInstanceId(instanceId)
                    .setCredentialsProvider(() -> credentials)
                    .build();
            return BigtableTableAdminClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Failed to connect to bigtable", e);
        }
    }

    static BigtableDataClient createBigtableDataClient(Config bigtableConfig) {
        String projectId = bigtableConfig.get("project-id").asString().orElse("my-project");
        String instanceId = bigtableConfig.get("instance-id").asString().orElse("my-instance");
        if (bigtableConfig.get("emulator").asBoolean().orElse(true)) {
            String host = bigtableConfig.get("host").asString().orElse("localhost");
            int port = bigtableConfig.get("port").asInt().orElse(9035);
            LOG.info("Creating Bigtable emulator data-client");
            return createEmulatorBigtableDataClient(host, port, projectId, instanceId);
        } else {
            Path serviceAccountKeyFilePath = Path.of(bigtableConfig.get("service-account.path").asString()
                    .orElseThrow(() -> new RuntimeException("'service-account.path' missing"))
            );
            LOG.info("Creating Bigtable data-client");
            return createRealBigtableDataClient(serviceAccountKeyFilePath, projectId, instanceId);
        }
    }

    static BigtableDataClient createEmulatorBigtableDataClient(String host, Integer port, String projectId, String instanceId) {
        try {
            BigtableDataSettings settings = BigtableDataSettings
                    .newBuilderForEmulator(host, port)
                    .setProjectId(projectId)
                    .setInstanceId(instanceId)
                    .build();
            return BigtableDataClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Could not connect to bigtable", e);
        }
    }

    static BigtableDataClient createRealBigtableDataClient(Path serviceAccountKeyFilePath, String projectId, String instanceId) {
        try {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(
                    Files.newInputStream(serviceAccountKeyFilePath, StandardOpenOption.READ))
                    .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            BigtableDataSettings settings = BigtableDataSettings
                    .newBuilder()
                    .setProjectId(projectId)
                    .setInstanceId(instanceId)
                    .setCredentialsProvider(() -> credentials)
                    .build();
            return BigtableDataClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Could not connect to bigtable", e);
        }
    }
}
