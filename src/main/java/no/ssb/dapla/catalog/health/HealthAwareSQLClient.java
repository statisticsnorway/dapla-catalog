package no.ssb.dapla.catalog.health;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.reactivex.ext.sql.SQLClient;
import io.vertx.reactivex.ext.sql.SQLOperations;
import io.vertx.reactivex.ext.sql.SQLRowStream;

import java.util.concurrent.atomic.AtomicReference;

public class HealthAwareSQLClient extends SQLClient {

    final SQLClient sqlClientDelegate;
    final AtomicReference<ReadinessSample> lastReadySample;

    class HealthSamplingHandler<R> implements Handler<AsyncResult<R>> {
        final Handler<AsyncResult<R>> delegatatedHandler;

        HealthSamplingHandler(Handler<AsyncResult<R>> delegatatedHandler) {
            this.delegatatedHandler = delegatatedHandler;
        }

        @Override
        public void handle(AsyncResult<R> ar) {
            lastReadySample.set(new ReadinessSample(ar.succeeded(), System.currentTimeMillis()));
            delegatatedHandler.handle(ar);
        }
    }

    public HealthAwareSQLClient(SQLClient sqlClientDelegate, AtomicReference<ReadinessSample> lastReadySample) {
        super(sqlClientDelegate.getDelegate());
        this.sqlClientDelegate = sqlClientDelegate;
        this.lastReadySample = lastReadySample;
    }

    @Override
    public SQLClient query(String sql, Handler<AsyncResult<io.vertx.ext.sql.ResultSet>> resultHandler) {
        return sqlClientDelegate.query(sql, new HealthSamplingHandler(resultHandler));
    }

    @Override
    public SQLClient queryWithParams(String sql, JsonArray params, Handler<AsyncResult<io.vertx.ext.sql.ResultSet>> resultHandler) {
        return sqlClientDelegate.queryWithParams(sql, params, new HealthSamplingHandler(resultHandler));
    }

    @Override
    public SQLClient queryStream(String sql, Handler<AsyncResult<SQLRowStream>> handler) {
        return sqlClientDelegate.queryStream(sql, new HealthSamplingHandler(handler));
    }

    @Override
    public SQLClient queryStreamWithParams(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> handler) {
        return sqlClientDelegate.queryStreamWithParams(sql, params, new HealthSamplingHandler(handler));
    }

    @Override
    public SQLOperations querySingle(String sql, Handler<AsyncResult<JsonArray>> handler) {
        return sqlClientDelegate.querySingle(sql, new HealthSamplingHandler(handler));
    }

    @Override
    public SQLOperations querySingleWithParams(String sql, JsonArray arguments, Handler<AsyncResult<JsonArray>> handler) {
        return sqlClientDelegate.querySingleWithParams(sql, arguments, new HealthSamplingHandler(handler));
    }

    @Override
    public SQLClient update(String sql, Handler<AsyncResult<io.vertx.ext.sql.UpdateResult>> resultHandler) {
        return sqlClientDelegate.update(sql, new HealthSamplingHandler(resultHandler));
    }

    @Override
    public SQLClient updateWithParams(String sql, JsonArray params, Handler<AsyncResult<io.vertx.ext.sql.UpdateResult>> resultHandler) {
        return sqlClientDelegate.updateWithParams(sql, params, new HealthSamplingHandler(resultHandler));
    }

    @Override
    public SQLClient call(String sql, Handler<AsyncResult<io.vertx.ext.sql.ResultSet>> resultHandler) {
        return sqlClientDelegate.call(sql, new HealthSamplingHandler(resultHandler));
    }

    @Override
    public SQLClient callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<io.vertx.ext.sql.ResultSet>> resultHandler) {
        return sqlClientDelegate.callWithParams(sql, params, outputs, new HealthSamplingHandler(resultHandler));
    }
}
