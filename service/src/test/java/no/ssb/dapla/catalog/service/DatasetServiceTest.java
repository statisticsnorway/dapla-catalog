package no.ssb.dapla.catalog.service;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.grpc.Channel;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.repository.DatasetRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static no.ssb.dapla.catalog.DatasetAssert.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class DatasetServiceTest {

    @Inject
    Application application;

    @Inject
    Channel channel;

    @AfterEach
    public void afterEach() {
        application.get(BigtableTableAdminClient.class).dropAllRows("dataset");
    }

    void createDataset(Dataset dataset) {
        application.get(DatasetRepository.class).create(dataset);
    }

    @Test
    void thatGetDatasetWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId("1")
                .setValuation(Valuation.SHIELDED)
                .setState(DatasetState.OUTPUT)
                .addLocations("f1")
                .build();
        createDataset(dataset);

        GetDatasetResponse response = CatalogServiceGrpc.newBlockingStub(channel).get(GetDatasetRequest.newBuilder().setId("1").build());

        assertThat(response.getDataset()).isEqualTo(dataset);
    }
}
