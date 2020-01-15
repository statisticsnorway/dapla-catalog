package no.ssb.dapla.catalog.dataset;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class NameIndex {
    private static final Logger LOG = LoggerFactory.getLogger(NameIndex.class);

    public static final String TABLE_ID = "name_index";
    public static final String COLUMN_FAMILY = "document";
    static final String COLUMN_QUALIFIER = "dataset_id";

    private BigtableDataClient dataClient;

    public NameIndex(BigtableDataClient dataClient) {
        this.dataClient = dataClient;
    }

    public CompletableFuture<String> mapNameToId(String name, String proposedId) {
        CompletableFuture<String> result = new CompletableFuture<>();
        ConditionalRowMutation mutation = ConditionalRowMutation.create(TABLE_ID, name)
                .condition(FILTERS.key().exactMatch(name))
                .otherwise(Mutation.create().setCell(COLUMN_FAMILY, COLUMN_QUALIFIER, proposedId));
        ApiFuture<Boolean> future = dataClient.checkAndMutateRowAsync(mutation);
        ApiFutures.addCallback(future, new ApiFutureCallback<>() {
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }

            public void onSuccess(Boolean wasApplied) {
                if (wasApplied) {
                    LOG.trace("Dataset name: \"{}\" was already mapped to an existing id. reading that id now.", name);
                    mapNameToId(name)
                            .thenAccept(result::complete)
                            .exceptionally(t -> {
                                result.completeExceptionally(t);
                                return null;
                            });
                } else {
                    result.complete(proposedId);
                    LOG.trace("Dataset name: \"{}\" was mapped to proposed id: {}", name, proposedId);
                }
            }
        }, MoreExecutors.directExecutor());
        return result;
    }

    public CompletableFuture<String> mapNameToId(String name) {
        CompletableFuture<String> future = new CompletableFuture<>();
        ApiFutures.addCallback(
                dataClient.readRowsCallable().first()
                        .futureCall(Query
                                .create(TABLE_ID)
                                .rowKey(name)
                                .limit(1)
                        ),
                new ApiFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onSuccess(Row result) {
                        if (result == null) {
                            LOG.trace("Dataset name: \"{}\" was not mapped to anything, returning empty.", name);
                            future.complete(null);
                            return;
                        }
                        List<RowCell> cells = result.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER);
                        RowCell cell = cells.get(0);
                        LOG.trace("Dataset name: \"{}\" was mapped to an existing id, returning that now.", name);
                        future.complete(cell.getValue().toStringUtf8());
                    }
                },
                MoreExecutors.directExecutor()
        );
        return future;
    }

    public CompletableFuture<Void> deleteMappingFor(String name) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ApiFutures.addCallback(dataClient.mutateRowCallable().futureCall(RowMutation.create(TABLE_ID, name, Mutation.create().deleteRow())),
                new ApiFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onSuccess(Void result) {
                        future.complete(null);
                        LOG.trace("Dataset name: \"{}\" was mapped to an existing id, returning that now.", name);
                    }
                },
                MoreExecutors.directExecutor()
        );
        return future;
    }
}
