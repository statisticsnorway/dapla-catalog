package no.ssb.dapla.catalog.service;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.grpc.Channel;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.DatasetAssert;
import no.ssb.dapla.catalog.IntegrationTestExtension;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.Dataset.DatasetState;
import no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.repository.DatasetRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
        try {
            application.get(DatasetRepository.class).create(dataset).get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
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

        DatasetAssert.assertThat(response.getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatGetDoesntReturnADatasetWhenOneDoesntExist() {
        GetDatasetResponse response = CatalogServiceGrpc.newBlockingStub(channel).get(
                GetDatasetRequest.newBuilder().setId("does_not_exist").build()
        );
        Assertions.assertThat(response.hasDataset()).isFalse();
    }
}
