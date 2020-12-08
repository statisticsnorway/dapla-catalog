package no.ssb.dapla.catalog.dataset;

import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;
import io.helidon.dbclient.DbClient;
import io.helidon.metrics.RegistryFactory;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Pattern;

public class DatasetRepository {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetRepository.class);

    private final DbClient client;

    private final Timer listTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.list");
    private final Timer readTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.read");
    private final Timer writeTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.write");
    private final Timer deleteTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.delete");

    public DatasetRepository(DbClient client) {
        this.client = client;
    }

    public static String replaceInvalidChars(String original) {
        // TODO: Constants.
        var invalidCharPattern = Pattern.compile("[^\\w]+");
        return invalidCharPattern.matcher(original).replaceAll("_");
    }

    public Multi<DatasetId> listByPrefix(String prefix, int limit) {
        return client.execute(exec -> exec.query("""
                        SELECT DISTINCT ON (path) path, version, document::JSON
                        FROM Dataset WHERE path LIKE ?
                        ORDER BY path, version DESC LIMIT ?""",
                prefix + "%", limit)
                .map(dbRow -> ProtobufJsonUtils.toPojo(dbRow.column(3).as(String.class), Dataset.class))
                .map(Dataset::getId)
        );
    }

    public Multi<Dataset> listDatasets(String pathPart, int limit) {
        return client.execute(exec -> exec.query("""
                        SELECT DISTINCT ON (path) path, document::JSON
                        FROM Dataset
                        WHERE path LIKE ?
                        ORDER BY path, version DESC LIMIT ?""",
                (pathPart != null && pathPart.length() > 0) ? "%" + pathPart + "%" : "%", limit)
                .map(dbRow -> ProtobufJsonUtils.toPojo(dbRow.column(2).as(String.class), Dataset.class))
        );
    }

    /**
     * Get the latest dataset with the given path
     */
    public Single<Dataset> get(String path) {
        return get(path, System.currentTimeMillis());
    }

    /**
     * Get the dataset that was the most recent at a given time
     */
    public Single<Dataset> get(String path, long timestamp) {
        return client.execute(exec -> exec.get("""
                        SELECT document::JSON
                        FROM Dataset
                        WHERE path = ? AND version <= ?
                        ORDER BY version DESC LIMIT 1""",
                path, LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC))
                .flatMapSingle(dbRowOpt -> dbRowOpt
                        .map(dbRow -> Single.just(ProtobufJsonUtils.toPojo(dbRow.column(1).as(String.class), Dataset.class)))
                        .orElseGet(Single::empty))
        );
    }

    public Single<Long> create(Dataset dataset) {
        String jsonDoc = ProtobufJsonUtils.toString(dataset);
        long now = System.currentTimeMillis();
        long effectiveTimestamp = dataset.getId().getTimestamp() == 0 ? now : dataset.getId().getTimestamp();
        return client.execute(exec -> exec.insert("""
                        INSERT INTO Dataset(path, version, document) VALUES(?, ?, ?::JSON)
                        ON CONFLICT (path, version) DO UPDATE SET document = ?::JSON""",
                dataset.getId().getPath(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(effectiveTimestamp), ZoneOffset.UTC),
                jsonDoc,
                jsonDoc));
    }

    public Single<Long> delete(String path, long timestamp) {
        return client.execute(exec -> exec.delete("DELETE FROM Dataset WHERE path = ? AND version = ?", path,
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC)));
    }

    public Single<Long> deleteAllDatasets() {
        return client.execute(exec -> exec.dml("TRUNCATE TABLE Dataset"));
    }

    public Multi<Dataset> listByPrefixAndDepth(String prefix, String depth, Integer limit) {

        var ltreePrefix = replaceInvalidChars(prefix);

        // The ltree does not support special chars so we still rely on the
        // path to filter out false positives.

        return client.execute(exec -> exec.createQuery("""
                SELECT * 
                FROM Dataset 
                WHERE lpath ~ :lprefix_wildcard
                  AND nlevel(lpath) <= nlevel(:lprefix) + :depth
                  AND path LIKE :prefix
                LIMIT :limit
                """
        )
                .addParam(":lprefix_wildcard", ltreePrefix + ".*")
                .addParam(":lprefix", ltreePrefix)
                .addParam(":prefix", prefix + '%')
                .addParam(":depth", depth)
                .addParam(":limit", limit)
                .execute()
        ).map(dbRow -> ProtobufJsonUtils.toPojo(dbRow.column("document").as(String.class), Dataset.class));
    }
}
