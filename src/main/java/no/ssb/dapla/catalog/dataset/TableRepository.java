package no.ssb.dapla.catalog.dataset;

import io.helidon.common.reactive.Single;
import io.helidon.dbclient.DbClient;
import no.ssb.dapla.catalog.protobuf.CatalogTable;

import static no.ssb.dapla.catalog.dataset.NamespaceUtils.escapePath;
import static no.ssb.dapla.catalog.dataset.NamespaceUtils.unescapePath;

public class TableRepository {

    private final DbClient client;

    public TableRepository(DbClient client) {
        this.client = client;
    }

    /**
     * Get the dataset that was the most recent at a given time
     */
    public Single<CatalogTable> get(String path) {
        return client.execute(exec -> exec.get("""
                        SELECT path, metadata_location
                        FROM catalog_table
                        WHERE path = ltree(?)
                        LIMIT 1""",
                escapePath(path))
                .flatMapSingle(dbRowOpt -> dbRowOpt
                        .map(dbRow -> Single.just(CatalogTable.newBuilder()
                                .setPath(unescapePath(dbRow.column(1).as(String.class)))
                                .setMetadataLocation(dbRow.column(2).as(String.class))
                                .build()))
                        .orElseGet(Single::empty))
        );
    }

    public Single<Long> create(CatalogTable catalogTable) {
        return client.execute(exec -> exec.insert("""
                        INSERT INTO catalog_table(path, metadata_location) VALUES(ltree(?), ?)
                        """,
                escapePath(catalogTable.getPath()), catalogTable.getMetadataLocation()));
    }

    public Single<Long> update(CatalogTable catalogTable, String previousMetadataLocation) {
        return client.execute(exec -> exec.update("""
                        UPDATE catalog_table SET metadata_location = ?,
                        previous_metadata_location = ?
                        WHERE path = ltree(?)
                        AND metadata_location = ?
                        """,
                catalogTable.getMetadataLocation(), previousMetadataLocation,
                escapePath(catalogTable.getPath()), previousMetadataLocation));
    }

    Single<Long> deleteAll() {
        return client.execute(exec -> exec.dml("TRUNCATE TABLE catalog_table"));
    }
}
