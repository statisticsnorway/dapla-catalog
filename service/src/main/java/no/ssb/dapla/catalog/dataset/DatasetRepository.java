package no.ssb.dapla.catalog.dataset;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.helidon.metrics.RegistryFactory;
import no.ssb.dapla.catalog.protobuf.Dataset;

import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.Meter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DatasetRepository {

    private BigtableDataClient dataClient;

    private static final String TABLE_ID = "dataset";
    private static final String COLUMN_FAMILY = "document";
    private static final String COLUMN_QUALIFIER = "dataset";

    private final Counter createDatasetCounter = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).counter("createDatasetCounter");
    private final Counter deleteDatasetCounter = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).counter("deleteDatasetCounter");

    private final Meter datasetCreateMeter = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).meter("datasetCreateMeter");
    private final Meter datasetDeleteMeter = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).meter("datasetDeleteMeter");

    private final Timer datasetCreateTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("datasetCreateTimer");
    private final Timer datasetDeleteTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("datasetDeleteTimer");

    public DatasetRepository(BigtableDataClient dataClient) {
        this.dataClient = dataClient;
    }

    static class FirstDatasetReaderApiFutureCallback implements ApiFutureCallback<Row> {
        final CompletableFuture<Dataset> future;

        FirstDatasetReaderApiFutureCallback(CompletableFuture<Dataset> future) {
            this.future = future;
        }

        @Override
        public void onFailure(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onSuccess(Row row) {
            if (row == null) {
                future.complete(null);
                return;
            }
            try {
                future.complete(Dataset.parseFrom(row.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER).get(0).getValue()));
            } catch (InvalidProtocolBufferException e) {
                future.completeExceptionally(e);
            }
        }
    }

    /**
     * Get the latest dataset with the given id
     */
    public CompletableFuture<Dataset> get(String id) {
        CompletableFuture<Dataset> future = new CompletableFuture<>();
        ApiFutures.addCallback(
                dataClient.readRowsCallable().first().futureCall(Query.create(TABLE_ID).prefix(id).limit(1)),
                new FirstDatasetReaderApiFutureCallback(future),
                MoreExecutors.directExecutor()
        );
        return future;
    }

    /**
     * Get the dataset that was the most recent at a given time
     */
    public CompletableFuture<Dataset> get(String id, long timestamp) {
        String start = String.format("%s#%d", id, Long.MAX_VALUE - timestamp);
        CompletableFuture<Dataset> future = new CompletableFuture<>();
        ApiFutures.addCallback(
                dataClient.readRowsCallable().first().futureCall(Query.create(TABLE_ID).range(start, null).limit(1)),
                new FirstDatasetReaderApiFutureCallback(future),
                MoreExecutors.directExecutor()
        );
        return future;
    }

    public CompletableFuture<Void> create(Dataset dataset) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        // We create a reverse timestamp so that the most recent dataset will appear at the start of the table instead
        // of the end. This because we assume that the most common query will be for the latest dataset.
        Long reverseTimestamp = Long.MAX_VALUE - System.currentTimeMillis();

        RowMutation rowMutation = RowMutation.create(TABLE_ID, String.format("%s#%d", dataset.getId(), reverseTimestamp))
                .setCell(
                        COLUMN_FAMILY,
                        ByteString.copyFrom(COLUMN_QUALIFIER.getBytes()),
                        dataset.toByteString()
                );

        ApiFutures.addCallback(dataClient.mutateRowAsync(rowMutation), new ApiFutureCallback<>() {
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
            }

            public void onSuccess(Void ignored) {
                future.complete(ignored);
                createDatasetCounter.inc();
                datasetCreateMeter.mark();
                datasetCreateTimer.time();
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    public CompletableFuture<Integer> delete(String id) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        ApiFutures.addCallback(dataClient.readRowsCallable().all().futureCall(Query.create(TABLE_ID).prefix(id)), new ApiFutureCallback<>() {
            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onSuccess(List<Row> rows) {
                if (rows.isEmpty()) {
                    future.complete(0);
                    return;
                }
                BulkMutation batch = BulkMutation.create(TABLE_ID);
                for (Row row : rows) {
                    batch.add(row.getKey(), Mutation.create().deleteRow());
                }
                ApiFutures.addCallback(dataClient.bulkMutateRowsAsync(batch), new ApiFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onSuccess(Void ignored) {
                        future.complete(rows.size());
                        deleteDatasetCounter.inc();
                        datasetDeleteMeter.mark();
                        datasetDeleteTimer.time();
                    }
                }, MoreExecutors.directExecutor());
            }
        }, MoreExecutors.directExecutor());
        return future;
    }
}
