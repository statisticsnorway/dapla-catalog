package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(IntegrationTestExtension.class)
class NameIndexTest {

    @Inject
    Application application;

    @BeforeEach
    public void beforeEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows(NameIndex.TABLE_ID);
    }

    @Test
    public void thatMapToIdWorks() {
        assertNull(application.get(NameIndex.class)
                .mapNameToId("/some/components/leading/to/name")
                .orTimeout(5, TimeUnit.SECONDS).join());

        assertEquals("firstMappedId", application.get(NameIndex.class)
                .mapNameToId("/some/components/leading/to/name", "firstMappedId")
                .orTimeout(5, TimeUnit.SECONDS).join());

        assertEquals("firstMappedId", application.get(NameIndex.class)
                .mapNameToId("/some/components/leading/to/name", "anotherId")
                .orTimeout(5, TimeUnit.SECONDS).join());

        assertEquals("firstMappedId", application.get(NameIndex.class)
                .mapNameToId("/some/components/leading/to/name")
                .orTimeout(5, TimeUnit.SECONDS).join());
    }
}