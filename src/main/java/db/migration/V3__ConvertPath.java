package db.migration;

import no.ssb.dapla.catalog.dataset.DatasetRepository;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

public class V3__ConvertPath extends BaseJavaMigration {

    private static final String SELECT = "SELECT path, version FROM dataset;";
    private static final String UPDATE = "UPDATE dataset SET path_ltree = ltree(?) WHERE path = ? AND version = ?;\n";

    @Override
    public void migrate(Context context) throws Exception {
        try (
                var select = context.getConnection().createStatement();
                var update = context.getConnection().prepareStatement(UPDATE)
        ) {
            try (var rows = select.executeQuery(SELECT)) {
                while (rows.next()) {
                    var path = rows.getString("path");
                    var version = rows.getTimestamp("version");

                    update.setString(1, DatasetRepository.escapePath(path));
                    update.setString(2, path);
                    update.setTimestamp(3, version);
                    update.execute();

                }
            }
        }

    }
}
