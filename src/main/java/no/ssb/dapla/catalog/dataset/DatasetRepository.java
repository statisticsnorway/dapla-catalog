package no.ssb.dapla.catalog.dataset;

import io.helidon.metrics.RegistryFactory;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.reactivex.ext.sql.SQLClient;
import io.vertx.reactivex.ext.sql.SQLRowStream;
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

public class DatasetRepository {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetRepository.class);

    private final SQLClient client;

    private final Timer listTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.list");
    private final Timer readTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.read");
    private final Timer writeTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.write");
    private final Timer deleteTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("dataset.repository.delete");

    public DatasetRepository(SQLClient client) {
        this.client = client;
    }

    public Flowable<DatasetId> listByPrefix(String prefix, int limit) {
        JsonArray params = new JsonArray().add(prefix + "%").add(limit);
        return client
                .rxQueryStreamWithParams("SELECT DISTINCT ON (path) " +
                        " path, version, document FROM Dataset WHERE path LIKE ? " +
                        " ORDER BY path, version DESC " +
                        " LIMIT ?", params)
                .flatMapPublisher(SQLRowStream::toFlowable)
                .map(jsonArray ->
                    ProtobufJsonUtils.toPojo(jsonArray.getString(2), Dataset.class))
                .map(Dataset::getId);
    }

    public Single<Integer> setDirtyPath(String path) {
        JsonArray params = new JsonArray().add( (path != null && path.length() >0) ? "%" + path + "%" : "%");
        return client
                .rxUpdateWithParams("update dataset set isdirty = 1 where path LIKE ?", params)
                .map(UpdateResult::getUpdated);
    }

    public Flowable<Dataset> listDatasets(String pathPart, int limit) {
        JsonArray params = new JsonArray().add( (pathPart != null && pathPart.length() >0) ? "%" + pathPart + "%" : "%").add(limit);
        return client
                .rxQueryStreamWithParams("SELECT DISTINCT ON (path) " +
                        " path, document, isDirty FROM Dataset WHERE path LIKE ? " +
                        " ORDER BY path, version DESC " +
                        " LIMIT ?", params)
                .flatMapPublisher(SQLRowStream::toFlowable)
                .map(jsonArray ->
                        ProtobufJsonUtils.toPojo(jsonArray.getString(1), Dataset.class))
                ;
    }

    /**
     * Get the latest dataset with the given path
     */
    public Maybe<Dataset> get(String path) {
        return get(path, System.currentTimeMillis());
    }

    /**
     * Get the dataset that was the most recent at a given time
     */
    public Maybe<Dataset> get(String path, long timestamp) {
        JsonArray params = new JsonArray()
                .add(path)
                .add(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC).toString());
        return client
                .rxQuerySingleWithParams("SELECT document FROM Dataset WHERE path = ? AND version <= ? ORDER BY version DESC LIMIT 1", params)
                .map(jsonArray -> ProtobufJsonUtils.toPojo(jsonArray.getString(0), Dataset.class));
    }

    public Single<Integer> create(Dataset dataset) {
        String jsonDoc = ProtobufJsonUtils.toString(dataset);
        long effectiveTimestamp = dataset.getId().getTimestamp() == 0 ? System.currentTimeMillis() : dataset.getId().getTimestamp();
        JsonArray params = new JsonArray()
                .add(dataset.getId().getPath())
                .add(LocalDateTime.ofInstant(Instant.ofEpochMilli(effectiveTimestamp), ZoneOffset.UTC).toString())
                .add(jsonDoc)
                .add(jsonDoc);
        return client
                .rxUpdateWithParams("INSERT INTO Dataset(path, version, document) VALUES(?, ?, ?) ON CONFLICT (path, version) DO UPDATE SET document = ?", params)
                .map(UpdateResult::getUpdated);
    }

    public Single<Integer> delete(String id) {
        JsonArray params = new JsonArray().add(id);
        return client
                .rxUpdateWithParams("DELETE FROM Dataset WHERE path = ?", params)
                .map(UpdateResult::getUpdated);
    }

    public Completable deleteAllDatasets() {
        return client.rxUpdate("TRUNCATE TABLE Dataset").ignoreElement();
    }
}
