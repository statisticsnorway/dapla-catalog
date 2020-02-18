package no.ssb.dapla.catalog.dataset;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.Application;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.dapla.catalog.protobuf.SecretPseudoConfigItem;
import no.ssb.dapla.catalog.protobuf.VarPseudoConfigItem;
import no.ssb.testing.helidon.GrpcMockRegistry;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@GrpcMockRegistryConfig(CatalogGrpcServiceTest.DatasetAccessTestGrpcMockRegistry.class)
@ExtendWith(IntegrationTestExtension.class)
class CatalogGrpcServiceTest {

    private static final Set<String> ACCESS = Set.of("a-user");

    @Inject
    Application application;

    @Inject
    Channel channel;

    @BeforeEach
    public void beforeEach() {
        application.get(DatasetRepository.class).deleteAllDatasets().blockingGet();
    }

    public static class DatasetAccessTestGrpcMockRegistry extends GrpcMockRegistry {
        public DatasetAccessTestGrpcMockRegistry() {
            add(new AuthServiceGrpc.AuthServiceImplBase() {
                @Override
                public void hasAccess(AccessCheckRequest request, StreamObserver<AccessCheckResponse> responseObserver) {
                    AccessCheckResponse.Builder responseBuilder = AccessCheckResponse
                            .newBuilder()
                            .setAllowed(ACCESS.contains(request.getUserId()));
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }
            });
        }
    }

    void repositoryCreate(Dataset dataset) {
        application.get(DatasetRepository.class).create(dataset).timeout(3, TimeUnit.SECONDS).blockingGet();
    }

    Dataset repositoryGet(String id) {
        return application.get(DatasetRepository.class).get(id).timeout(3, TimeUnit.SECONDS).blockingGet();
    }

    GetDatasetResponse get(String path) {
        return CatalogServiceGrpc.newBlockingStub(channel).get(GetDatasetRequest.newBuilder().setPath(path).build());
    }

    GetDatasetResponse get(String path, long timestamp) {
        return CatalogServiceGrpc.newBlockingStub(channel).get(GetDatasetRequest.newBuilder().setPath(path).setTimestamp(timestamp).build());
    }

    SaveDatasetResponse save(Dataset dataset, String userId) {
        return CatalogServiceGrpc.newBlockingStub(channel).save(SaveDatasetRequest.newBuilder().setDataset(dataset).setUserId(userId).build());
    }

    DeleteDatasetResponse delete(String id) {
        return CatalogServiceGrpc.newBlockingStub(channel).delete(DeleteDatasetRequest.newBuilder().setId(id).build());
    }

    ListByPrefixResponse listByPrefix(String prefix, int limit) {
        return CatalogServiceGrpc.newBlockingStub(channel).listByPrefix(ListByPrefixRequest.newBuilder().setPrefix(prefix).setLimit(limit).build());
    }

    @Test
    void thatListByPrefixWorks() {
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/another/prefix").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/unit-test/and/other/data").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/unit-test/with/data/1").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/unit-test/with/data/2").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/unit-test/with/data/3").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/unitisgood/forall").build()).build());
        repositoryCreate(Dataset.newBuilder().setId(DatasetId.newBuilder().setPath("/x-after/and/more/data").build()).build());
        ListByPrefixResponse response = listByPrefix("/unit", 100);
        List<DatasetId> entries = response.getEntriesList();
        assertThat(entries.size()).isEqualTo(5);
        assertThat(entries.get(0).getPath()).isEqualTo("/unit-test/and/other/data");
        assertThat(entries.get(1).getPath()).isEqualTo("/unit-test/with/data/1");
        assertThat(entries.get(2).getPath()).isEqualTo("/unit-test/with/data/2");
        assertThat(entries.get(3).getPath()).isEqualTo("/unit-test/with/data/3");
        assertThat(entries.get(4).getPath()).isEqualTo("/unitisgood/forall");
    }

    @Test
    void thatGetDatasetWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("1").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.OUTPUT)
                .setPseudoConfig(dummyPseudoConfig())
                .setPhysicalLocation("f1")
                .build();
        repositoryCreate(dataset);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setPath("2").build())
                        .setValuation(Dataset.Valuation.SENSITIVE)
                        .setState(Dataset.DatasetState.RAW)
                        .setPseudoConfig(dummyPseudoConfig())
                        .setPhysicalLocation("file")
                        .build()
        );

        assertThat(get("1").getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatGetDoesntReturnADatasetWhenOneDoesntExist() {
        assertThat(get("does_not_exist").hasDataset()).isFalse();
    }

    @Test
    void thatGettingAPreviousDatasetWorks() throws InterruptedException {
        Dataset old = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("a_dataset").build())
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig(dummyPseudoConfig())
                .build();
        repositoryCreate(old);

        Thread.sleep(50L);

        long timestamp = System.currentTimeMillis();

        Thread.sleep(50L);

        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setPath("a_dataset").build())
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .setPhysicalLocation("a_location")
                        .build()
        );

        assertThat(get("a_dataset", timestamp).getDataset()).isEqualTo(old);
    }

    @Test
    void thatGetPreviousReturnsNothingWhenTimestampIsOld() {
        repositoryCreate(
                Dataset.newBuilder()
                        .setId(DatasetId.newBuilder().setPath("dataset_from_after_timestamp").build())
                        .setValuation(Dataset.Valuation.OPEN)
                        .setState(Dataset.DatasetState.RAW)
                        .setPhysicalLocation("a_location")
                        .build()
        );
        assertThat(get("dataset_from_after_timestamp", 100L).hasDataset()).isFalse();
    }

    @Test
    void thatGetPreviousReturnsTheLatestDatasetWhenTimestampIsAfterTheLatest() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("dataset_from_before_timestamp").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.PRODUCT)
                .setPseudoConfig(dummyPseudoConfig())
                .setPhysicalLocation("some_file")
                .build();
        repositoryCreate(dataset);

        long timestamp = System.currentTimeMillis() + 50;

        assertThat(get("dataset_from_before_timestamp", timestamp).getDataset()).isEqualTo(dataset);
    }

    @Test
    void thatDeleteWorks() {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("dataset_to_delete").build())
                .setValuation(Dataset.Valuation.OPEN)
                .setState(Dataset.DatasetState.RAW)
                .setPhysicalLocation("f")
                .build();
        repositoryCreate(dataset);
        delete(dataset.getId().getPath());
        assertThat(repositoryGet("dataset_to_delete")).isNull();
    }

    @Test
    void thatDeleteWorksWhenDatasetDoesntExist() {
        delete("does_not_exist");
    }

    Dataset createDataset(String datasetId, Dataset.DatasetState datasetState, Dataset.Valuation datasetValuation, PseudoConfig pseudoConfig, String location) {
        Dataset dataset = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath(datasetId).build())
                .setState(datasetState)
                .setValuation(datasetValuation)
                .setPseudoConfig(pseudoConfig)
                .setPhysicalLocation(location)
                .build();
        repositoryCreate(dataset);
        return dataset;
    }

    static PseudoConfig dummyPseudoConfig() {
        return PseudoConfig.newBuilder()
                .addVars(VarPseudoConfigItem.newBuilder().setVar("var1").setPseudoFunc("someFunc1(param1,keyId1)"))
                .addVars(VarPseudoConfigItem.newBuilder().setVar("var2").setPseudoFunc("someFunc2(keyId2)"))
                .addVars(VarPseudoConfigItem.newBuilder().setVar("var3").setPseudoFunc("someFunc3(keyId1)"))
                .addSecrets(SecretPseudoConfigItem.newBuilder().setId("keyId1"))
                .addSecrets(SecretPseudoConfigItem.newBuilder().setId("keyId2"))
                .build();
    }

    @Test
    void thatCreateWorksIfUserHasCreateAccess() {
        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("dataset_to_create").build())
                .setValuation(Dataset.Valuation.SENSITIVE)
                .setState(Dataset.DatasetState.OUTPUT)
                .setPseudoConfig(dummyPseudoConfig())
                .setPhysicalLocation("file_location")
                .build();
        save(ds1, "a-user");

        assertThat(repositoryGet("dataset_to_create")).isEqualTo(ds1);

        Dataset ds2 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("dataset_to_create").build())
                .setValuation(Dataset.Valuation.INTERNAL)
                .setState(Dataset.DatasetState.PROCESSED)
                .setPseudoConfig(dummyPseudoConfig())
                .setPhysicalLocation("file_location_2")
                .build();
        save(ds2, "a-user");

        assertThat(repositoryGet("dataset_to_create")).isEqualTo(ds2);
    }

    @Test
    void thatCreateFailsIfUserHasNoCreateAccess() {
        Dataset ds1 = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setPath("dataset_to_create").build())
                .setValuation(Dataset.Valuation.SENSITIVE)
                .setState(Dataset.DatasetState.OUTPUT)
                .setPseudoConfig(dummyPseudoConfig())
                .setPhysicalLocation("file_location")
                .build();

        Assertions.assertThrows(StatusRuntimeException.class, () -> {
            save(ds1, "b-user");
        });
    }
}
