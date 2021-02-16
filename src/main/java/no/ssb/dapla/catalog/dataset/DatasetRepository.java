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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatasetRepository {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetRepository.class);

    private static final Pattern CODEPOINT = Pattern.compile("_[0-9]{4}");
    private static final Pattern VALID_CHARS = Pattern.compile("([^\\w]|_)+");

    private final DbClient client;

    private final Timer listTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.list");
    private final Timer readTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.read");
    private final Timer writeTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.write");
    private final Timer deleteTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.delete");

    public DatasetRepository(DbClient client) {
        this.client = client;
    }

    /**
     * Unescape all characters that could cause problems with the ltree column.
     */
    public static String unescapePath(String path) {
        var escaped = Stream.of(path.split("\\."))
                .map(part -> {
                    return CODEPOINT.matcher(part).replaceAll(match -> {
                        var point = Integer.parseInt(match.group().substring(1, 5));
                        return String.valueOf(Character.toChars(point));
                    });
                })
                .collect(Collectors.joining("/"));
        if (!escaped.startsWith("/")) {
            escaped = "/" + escaped;
        }
        return escaped;
    }

    /**
     * Escape all characters that could cause problems with the ltree column.
     */
    public static String escapePath(String path) {
        var escaped = Stream.of(path.split("/"))
                .map(part -> {
                    return VALID_CHARS.matcher(part).replaceAll(match -> {
                        return match.group().codePoints()
                                .mapToObj(codePoint -> String.format("!%04d", codePoint))
                                .collect(Collectors.joining());
                    });
                })
                .collect(Collectors.joining("."));
        if (escaped.startsWith(".")) {
            escaped = escaped.substring(1);
        }
        return escaped.replaceAll("!", "_");
    }

    // TODO: Limit is useless without offset.
    public Multi<DatasetId> listByPrefix(String prefix, int limit) {
        return client.execute(exec -> exec.query("""
                        SELECT DISTINCT ON (path) path, version, document::JSON
                        FROM Dataset WHERE path ~ lquery(?)
                        ORDER BY path, version DESC LIMIT ?""",
                escapePath(prefix) + "*.*", limit)
                .map(dbRow -> ProtobufJsonUtils.toPojo(dbRow.column(3).as(String.class), Dataset.class))
                .map(Dataset::getId)
        );
    }

    // TODO: Limit is useless without offset.
    public Multi<Dataset> listDatasets(String pathPart, int limit) {
        return client.execute(exec -> exec.query("""
                        SELECT DISTINCT ON (path) path, document::JSON
                        FROM Dataset
                        WHERE path <@ ltree(?)
                        ORDER BY path, version DESC LIMIT ?
                        """,
                pathPart != null && pathPart.length() > 0
                        ? escapePath(pathPart)
                        : "",
                limit
                ).map(dbRow -> ProtobufJsonUtils.toPojo(dbRow.column(2).as(String.class), Dataset.class))
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
                        WHERE path = ltree(?) AND version <= ?
                        ORDER BY version DESC LIMIT 1""",
                escapePath(path), Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC))
                .flatMapSingle(dbRowOpt -> dbRowOpt
                        .map(dbRow -> Single.just(ProtobufJsonUtils.toPojo(dbRow.column(1).as(String.class), Dataset.class)))
                        .orElseGet(Single::empty))
        );
    }

    public Single<Long> create(Dataset dataset) {
        String jsonDoc = ProtobufJsonUtils.toString(dataset);
        return client.execute(exec -> exec.insert("""
                        INSERT INTO Dataset(path, version, document) VALUES(ltree(?), ?, ?::JSON)
                        ON CONFLICT (path, version) DO UPDATE SET document = ?::JSON""",
                escapePath(dataset.getId().getPath()),
                Instant.ofEpochMilli(dataset.getId().getTimestamp()).atOffset(ZoneOffset.UTC),
                jsonDoc,
                jsonDoc));
    }

    public Single<Long> delete(String path, long timestamp) {
        return client.execute(exec -> exec.delete("""
                        DELETE FROM Dataset WHERE path = ltree(?) AND version = ?
                        """,
                escapePath(path),
                Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)));
    }

    public Single<Long> deleteAllDatasets() {
        return client.execute(exec -> exec.dml("TRUNCATE TABLE Dataset"));
    }

    // TODO: Limit is useless without offset.
    public Multi<DatasetId> listVersions(String path, Integer limit) {
        return client.execute(dbExecute -> dbExecute.createQuery("""
            SELECT path,
                   version
            FROM dataset
            WHERE path = ltree(:path)
            ORDER BY version DESC LIMIT :limit
        """)
                .addParam("path", escapePath(path))
                .addParam("limit", limit)
                .execute()
        ).map(row -> DatasetId.newBuilder()
                .setPath(unescapePath(row.column("path").as(String.class)))
                .setTimestamp(row.column("version").as(ZonedDateTime.class).toInstant().toEpochMilli())
                .build()
        );
    }

    // TODO: Limit is useless without offset.
    public Multi<DatasetId> listPathsByPrefix(String prefix, ZonedDateTime timestamp, Integer limit) {
        return client.execute(dbExecute -> dbExecute.createQuery("""
                        SELECT path,
                               MAX(version) as version
                        FROM dataset
                        WHERE version <= :version
                          AND path <@ ltree(:prefix)
                        GROUP BY path
                        LIMIT :limit
                        """
                )
                        .addParam("prefix", escapePath(prefix))
                        .addParam("version", timestamp.toOffsetDateTime())
                        .addParam("limit", limit)
                        .execute()
        ).map(row -> DatasetId.newBuilder()
                .setPath(unescapePath(row.column("path").as(String.class)))
                .setTimestamp(row.column("version").as(ZonedDateTime.class).toInstant().toEpochMilli())
                .build()
        );

    }

    // TODO: Limit is useless without offset.
    public Multi<DatasetId> listFoldersByPrefix(String prefix, ZonedDateTime timestamp, Integer limit) {
        return client.execute(dbExecute -> dbExecute.createQuery("""
                        SELECT subpath(path, 0, nlevel(ltree(:prefix)) + 1) as folder_path,
                               MAX(version)                                 as version
                        FROM dataset
                        WHERE version <= :version AND path <@ ltree(:prefix)
                          AND nlevel(ltree(:prefix)) + 1 < nlevel(path) 
                        GROUP BY folder_path 
                        ORDER BY folder_path
                        LIMIT :limit
                        """
        )
                .addParam("prefix", escapePath(prefix))
                // TODO: Bug in DbClient. Map needs to be the same size
                .addParam("dummy1", "")
                .addParam("dummy2", "")
                .addParam("version", timestamp.toOffsetDateTime())
                .addParam("limit", limit)
                .execute()
        ).map(row -> DatasetId.newBuilder()
                .setPath(unescapePath(row.column("folder_path").as(String.class)))
                .setTimestamp(row.column("version").as(ZonedDateTime.class).toInstant().toEpochMilli())
                .build()
        );

    }

    public Multi<Dataset> listDatasetsByPrefix(String prefix, ZonedDateTime timestamp, Integer limit) {
        return client.execute(exec -> exec.createQuery("""
                        WITH latest AS (
                            SELECT path,
                                   MAX(version) as version
                            FROM dataset
                            WHERE version <= :version
                              AND path <@ ltree(:prefix)
                              AND nlevel(path) <= nlevel(ltree(:prefix)) + 1
                            GROUP BY path
                        )
                        SELECT l.path,
                               l.version,
                               document
                        FROM latest l
                                 LEFT JOIN dataset d
                                            ON l.path = d.path AND l.version = d.version
                        ORDER BY l.path
                        LIMIT :limit
                        """
        )
                .addParam("prefix", escapePath(prefix))
                // TODO: Bug in DbClient. Map needs to be the same size
                .addParam("dummy1", "")
                .addParam("version", timestamp.toOffsetDateTime())
                .addParam("limit", limit)
                .execute()
        ).map(dbRow -> ProtobufJsonUtils.toPojo(dbRow.column("document").as(String.class), Dataset.class));
    }
}
