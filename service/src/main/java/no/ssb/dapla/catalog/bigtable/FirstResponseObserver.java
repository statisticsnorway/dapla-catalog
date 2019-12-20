package no.ssb.dapla.catalog.bigtable;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.models.Row;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class FirstResponseObserver<T> implements ResponseObserver<Row> {
    final CompletableFuture<T> future;
    final Function<Row, T> rowHandler;
    final AtomicReference<StreamController> controller = new AtomicReference<>();
    final AtomicInteger count = new AtomicInteger(0);

    public FirstResponseObserver(CompletableFuture<T> future, Function<Row, T> rowHandler) {
        this.future = future;
        this.rowHandler = rowHandler;
    }

    public void onStart(StreamController controller) {
        this.controller.set(controller);
    }

    public void onResponse(Row row) {
        if (count.incrementAndGet() > 1) {
            // second row
            controller.get().cancel();
            return;
        }
        // first row
        future.complete(rowHandler.apply(row));
    }

    public void onError(Throwable t) {
        future.completeExceptionally(t);
    }

    public void onComplete() {
        future.complete(null);
    }
}
