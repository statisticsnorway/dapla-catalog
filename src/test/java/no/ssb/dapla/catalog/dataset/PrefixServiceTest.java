package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.TestClient;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.NameAndIdEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class PrefixServiceTest {

    @Inject
    Application application;

    @Inject
    TestClient testClient;

    @BeforeEach
    public void beforeEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows(NameIndex.TABLE_ID);
    }

    @Test
    void thatGetByPrefixWorks() {
        NameIndex index = application.get(NameIndex.class);
        index.mapNameToId("/another/prefix", "another");
        index.mapNameToId("/unit-test/and/other/data", "other");
        index.mapNameToId("/unit-test/with/data/1", "1");
        index.mapNameToId("/unit-test/with/data/2", "2");
        index.mapNameToId("/unit-test/with/data/3", "3");
        index.mapNameToId("/unitisgood/forall", "me");
        index.mapNameToId("/x-after/and/more/data", "more");

        ListByPrefixResponse response = testClient.get("/prefix/unit", ListByPrefixResponse.class).expect200Ok().body();

        List<NameAndIdEntry> entries = response.getEntriesList();
        assertThat(entries.size()).isEqualTo(5);
        assertThat(NamespaceUtils.toNamespace(entries.get(0).getNameList())).isEqualTo("/unit-test/and/other/data");
        assertThat(entries.get(0).getId()).isEqualTo("other");
        assertThat(NamespaceUtils.toNamespace(entries.get(1).getNameList())).isEqualTo("/unit-test/with/data/1");
        assertThat(entries.get(1).getId()).isEqualTo("1");
        assertThat(NamespaceUtils.toNamespace(entries.get(2).getNameList())).isEqualTo("/unit-test/with/data/2");
        assertThat(entries.get(2).getId()).isEqualTo("2");
        assertThat(NamespaceUtils.toNamespace(entries.get(3).getNameList())).isEqualTo("/unit-test/with/data/3");
        assertThat(entries.get(3).getId()).isEqualTo("3");
        assertThat(NamespaceUtils.toNamespace(entries.get(4).getNameList())).isEqualTo("/unitisgood/forall");
        assertThat(entries.get(4).getId()).isEqualTo("me");
    }
}
