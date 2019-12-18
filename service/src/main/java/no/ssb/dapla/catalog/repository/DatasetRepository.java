package no.ssb.dapla.catalog.repository;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import no.ssb.dapla.catalog.protobuf.Dataset;

public class DatasetRepository {

    private BigtableDataClient dataClient;

    private static final String TABLE_ID = "dataset";
    private static final String COLUMN_FAMILY = "document";
    private static final String COLUMN_QUALIFIER = "dataset";

    public DatasetRepository(BigtableDataClient dataClient) {
        this.dataClient = dataClient;
    }

    /**
     * Get the latest dataset with the given id
     */
    public Dataset get(String id) throws InvalidProtocolBufferException {
        for (Row row : dataClient.readRows(Query.create(TABLE_ID).prefix(id).limit(1))) {
            return Dataset.parseFrom(row.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER).get(0).getValue());
        }
        return null;
    }

    /**
     * Get the dataset that was the most recent at a given time
     */
    public Dataset get(String id, long timestamp) throws InvalidProtocolBufferException {

        String start = String.format("%s#%d", id, Long.MAX_VALUE - timestamp);

        for (Row row : dataClient.readRows(Query.create(TABLE_ID).range(start, null).limit(1))) {
            return Dataset.parseFrom(row.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER).get(0).getValue());
        }
        return null;
    }

    public void create(Dataset dataset) {

        // We create a reverse timestamp so that the most recent dataset will appear at the start of the table instead
        // of the end. This because we assume that the most common query will be for the latest dataset.
        Long reverseTimestamp = Long.MAX_VALUE - System.currentTimeMillis();

        RowMutation rowMutation = RowMutation.create(TABLE_ID, String.format("%s#%d", dataset.getId(), reverseTimestamp))
                .setCell(
                        COLUMN_FAMILY,
                        ByteString.copyFrom(COLUMN_QUALIFIER.getBytes()),
                        dataset.toByteString()
                );

        dataClient.mutateRow(rowMutation);
    }

    public void update(Dataset dataset) {
        throw new RuntimeException("Not implemented");
    }

    public void delete(String id) {
        throw new RuntimeException("Not implemented");
    }
}
