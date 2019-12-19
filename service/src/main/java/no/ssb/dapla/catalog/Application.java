package no.ssb.dapla.catalog;

import ch.qos.logback.classic.util.ContextInitializer;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.helidon.config.Config;
import io.helidon.config.spi.ConfigSource;
import no.ssb.dapla.catalog.repository.DatasetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.LogManager;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;

public class Application {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList<>();
        String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
        if (overrideFile != null) {
            configSourceSupplierList.add(file(overrideFile).optional());
        }
        configSourceSupplierList.add(file("conf/application.yaml").optional());
        configSourceSupplierList.add(classpath("application.yaml"));

        Application application = new Application(Config.builder().sources(configSourceSupplierList).build());
    }

    public Application(Config config) {
        put(Config.class, config);

        BigtableTableAdminClient adminClient = createBigtableAdminClient(config.get("bigtable"));
        put(BigtableTableAdminClient.class, adminClient);
        createBigtableSchemaIfNotExists(config.get("bigtable"), adminClient);

        BigtableDataClient dataClient = createBigtableDataClient(config.get("bigtable"));
        put(BigtableDataClient.class, dataClient);

        DatasetRepository repository = new DatasetRepository(dataClient);
        put(DatasetRepository.class, repository);
    }

    private static void createBigtableSchemaIfNotExists(Config bigtableConfig, BigtableTableAdminClient adminClient) {
        String tableId = bigtableConfig.get("table-id").asString().orElse("dataset");
        if (!adminClient.exists(tableId)) {
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(bigtableConfig.get("column-family").asString().orElse("document"));
            adminClient.createTable(createTableRequest);
        }
    }

    private static BigtableTableAdminClient createBigtableAdminClient(Config bigtableConfig) {
        try {
            BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings
                    .newBuilderForEmulator(bigtableConfig.get("host").asString().orElse("localhost"), bigtableConfig.get("port").asInt().orElse(9035))
                    .setProjectId(bigtableConfig.get("project-id").asString().orElse("my-project"))
                    .setInstanceId(bigtableConfig.get("instance-id").asString().orElse("my-instance"))
                    //                .setCredentialsProvider() //TODO
                    .build();
            return BigtableTableAdminClient.create(adminSettings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Failed to connect to bigtable", e);
        }
    }

    private static BigtableDataClient createBigtableDataClient(Config bigtableConfig) {

        BigtableDataSettings dataSettings = BigtableDataSettings
                .newBuilderForEmulator(bigtableConfig.get("host").asString().orElse("localhost"), bigtableConfig.get("port").asInt().orElse(9035))
                .setProjectId(bigtableConfig.get("project-id").asString().orElse("my-project"))
                .setInstanceId(bigtableConfig.get("instance-id").asString().orElse("my-instance"))
//                .setCredentialsProvider() //TODO
                .build();

        try {
            return BigtableDataClient.create(dataSettings);
        } catch (IOException e) {
            throw new ApplicationInitializationException("Could not connect to bigtable", e);
        }
    }

    static final class ApplicationInitializationException extends RuntimeException {
        public ApplicationInitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public CompletionStage<Application> start() {
        throw new RuntimeException("Not implemented yet");
    }

    public Application stop() {
        get(BigtableDataClient.class).close();
        get(BigtableTableAdminClient.class).close();
        return this;
    }
}
