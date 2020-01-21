package no.ssb.dapla.catalog.dataset;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import no.ssb.dapla.catalog.Application;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
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
    void thatGetThenPostThenGetThenDeleteThenGetWorksAsExpected() {
        testClient.get("/name/MyName1").expect404NotFound();
        assertThat(testClient.post("/name/MyName1", "{ \"proposedId\": \"MyId1\"}").expect200Ok()
                .body()).contains("MyId1");
        assertThat(testClient.post("/name/MyName1", "{ \"proposedId\": \"otherId2\"}").expect200Ok()
                .body()).contains("MyId1").doesNotContain("otherId2");
        assertThat(testClient.get("/name/MyName1").expect200Ok().body()).contains("MyId1");
        assertThat(testClient.delete("/name/MyName1").expect200Ok().body()).isEmpty();
        testClient.get("/name/MyName1").expect404NotFound();
    }

    @Test
    void thatPostWithPathWorks() {
        testClient.post("/name/some/path/to/jane", "{ \"proposedId\": \"b32d47b0-e105-4c57-8537-b13ae45adfca\"}").expect200Ok()
                .body().contains("b32d47b0-e105-4c57-8537-b13ae45adfca");
        testClient.get("/name/some/path/to/jane").expect200Ok()
                .body().contains("b32d47b0-e105-4c57-8537-b13ae45adfca");
    }

    @Test
    void thatGetWithBothDecodedAndEncodedPathAsParamInURLWorks() {
        application.get(NameIndex.class).mapNameToId("/some/path/to/jane", "b32d47b0-e105-4c57-8537-b13ae45adfca").join();
        testClient.get("/name/some/path/to/jane").expect200Ok()
                .body().contains("b32d47b0-e105-4c57-8537-b13ae45adfca");
        testClient.get("/name/%2Fsome%2Fpath%2Fto%2Fjane").expect200Ok()
                .body().contains("b32d47b0-e105-4c57-8537-b13ae45adfca");
    }

    @Test
    void thatGetWithExtraLeadingSlashesInPathParamWorks() {
        application.get(NameIndex.class).mapNameToId("/some/path/to/jane", "b32d47b0-e105-4c57-8537-b13ae45adfca").join();
        testClient.get("/name///////%2F/////%2F%2F%2Fsome%2Fpath%2Fto%2Fjane").expect200Ok()
                .body().contains("b32d47b0-e105-4c57-8537-b13ae45adfca");
    }
}
