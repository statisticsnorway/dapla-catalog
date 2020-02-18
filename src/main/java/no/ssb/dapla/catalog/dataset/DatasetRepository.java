package no.ssb.dapla.catalog.dataset;

import io.helidon.metrics.RegistryFactory;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.reactivex.ext.sql.SQLClient;
import io.vertx.reactivex.ext.sql.SQLRowStream;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DatasetRepository {

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
                .rxQueryStreamWithParams("SELECT path, version, document FROM Dataset WHERE path LIKE ? LIMIT ?", params)
                .flatMapPublisher(SQLRowStream::toFlowable)
                .map(jsonArray -> ProtobufJsonUtils.toPojo(jsonArray.getString(2), Dataset.class))
                .map(Dataset::getId);
    }

    /**
     * Get the latest dataset with the given path
     */
    public Maybe<Dataset> get(String path) {
        JsonArray params = new JsonArray()
                .add(path);
        return client
                .rxQuerySingleWithParams("SELECT document FROM Dataset WHERE path = ? AND version <= LOCALTIMESTAMP ORDER BY version DESC LIMIT 1", params)
                .map(jsonArray -> ProtobufJsonUtils.toPojo(jsonArray.getString(0), Dataset.class));
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
        JsonObject document = (JsonObject) Json.decodeValue(jsonDoc);
        JsonArray params = new JsonArray()
                .add(dataset.getId().getPath())
                .add(LocalDateTime.now(ZoneOffset.UTC).toString())
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
