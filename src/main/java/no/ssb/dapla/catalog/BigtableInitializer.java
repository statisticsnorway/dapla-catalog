package no.ssb.dapla.catalog;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.helidon.config.Config;
import no.ssb.dapla.catalog.dataset.DatasetRepository;
import no.ssb.dapla.catalog.dataset.NameIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

public class BigtableInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableInitializer.class);

    private static GoogleCredentials getCredentials(Config bigtableConfig) {
        String configuredProviderChoice = bigtableConfig.get("credential-provider").asString().orElse("default");
        if ("service-account".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Running with the service-account google bigtable credentials provider");
            Path serviceAccountKeyFilePath = Path.of(bigtableConfig.get("credentials.service-account.path").asString()
                    .orElseThrow(() -> new RuntimeException("'credentials.service-account.path' missing from bigtable config"))
            );
            GoogleCredentials credentials;
            try {
                credentials = ServiceAccountCredentials.fromStream(
                        Files.newInputStream(serviceAccountKeyFilePath, StandardOpenOption.READ));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return credentials;
        } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Running with the compute-engine google bigtable credentials provider");
            return ComputeEngineCredentials.create();
        } else { // default
            LOG.info("Running with the default google bigtable credentials provider");
            return null;
        }
    }

    static void createBigtableSchemaIfNotExists(Config bigtableConfig, BigtableTableAdminClient adminClient) {
        if (!bigtableConfig.get("generate-schema").asBoolean().orElse(false)) {
            return;
        }
        LOG.info("Schema generation enabled");
        createTable(adminClient, DatasetRepository.TABLE_ID, DatasetRepository.COLUMN_FAMILY);
        createTable(adminClient, NameIndex.TABLE_ID, NameIndex.COLUMN_FAMILY);
    }

    private static void createTable(BigtableTableAdminClient adminClient, String tableId, String family) {
        if (!adminClient.exists(tableId)) {
            LOG.info("Generate table '{}' with family '{}'", tableId, family);
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(family);
            adminClient.createTable(createTableRequest);
        } else {
            LOG.info("table '{}' already exists", tableId);
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
            LOG.info("Creating Bigtable admin-client");
            GoogleCredentials credentials = getCredentials(bigtableConfig);
            return createRealBigtableTableAdminClient(credentials, projectId, instanceId);
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

    static BigtableTableAdminClient createRealBigtableTableAdminClient(GoogleCredentials credentials, String projectId, String instanceId) {
        try {
            BigtableTableAdminSettings.Builder builder = BigtableTableAdminSettings
                    .newBuilder()
                    .setProjectId(projectId)
                    .setInstanceId(instanceId);
            if (credentials != null) {
                builder.setCredentialsProvider(() -> credentials.createScoped(Collections.singletonList("https://www.googleapis.com/auth/bigtable.admin.table")));
            }
            BigtableTableAdminSettings settings = builder.build();
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
            LOG.info("Creating Bigtable data-client");
            GoogleCredentials credentials = getCredentials(bigtableConfig);
            return createRealBigtableDataClient(credentials, projectId, instanceId);
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

    static BigtableDataClient createRealBigtableDataClient(GoogleCredentials credentials, String projectId, String instanceId) {
        try {
            BigtableDataSettings.Builder builder = BigtableDataSettings
                    .newBuilder()
                    .setProjectId(projectId)
                    .setInstanceId(instanceId);
            if (credentials != null) {
                builder.setCredentialsProvider(() -> credentials.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")));
            }
            BigtableDataSettings settings = builder.build();
            return BigtableDataClient.create(settings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Could not connect to bigtable", e);
        }
    }
}
