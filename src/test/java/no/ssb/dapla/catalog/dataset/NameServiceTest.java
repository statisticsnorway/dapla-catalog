package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.TestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class NameServiceTest {

    @Inject
    Application application;

    @Inject
    TestClient testClient;

    @BeforeEach
    public void beforeEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows(NameIndex.TABLE_ID);
    }

    @Test
    void thatGetThenPostThenGetAgainWorksAsExpected() {
        testClient.get("/name/MyName1").expect404NotFound();
        assertThat(testClient.post("/name/MyName1/MyId1").expect200Ok().body()).contains("MyId1");
        assertThat(testClient.post("/name/MyName1/otherId2").expect200Ok().body()).contains("MyId1").doesNotContain("otherId2");
        assertThat(testClient.get("/name/MyName1").expect200Ok().body()).contains("MyId1");
    }
}
