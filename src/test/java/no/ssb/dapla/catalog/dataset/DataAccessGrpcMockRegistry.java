package no.ssb.dapla.catalog.dataset;


import io.grpc.stub.StreamObserver;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.testing.helidon.GrpcMockRegistry;

import java.util.Set;

public class DataAccessGrpcMockRegistry extends GrpcMockRegistry {

    private static final Set<String> ACCESS = Set.of("user");

    public DataAccessGrpcMockRegistry() {
        add(new AuthServiceGrpc.AuthServiceImplBase() {
            @Override
            public void hasAccess(AccessCheckRequest request, StreamObserver<AccessCheckResponse> responseObserver) {
                AccessCheckResponse.Builder responseBuilder = AccessCheckResponse.newBuilder();

                if (ACCESS.contains(request.getUserId())) {
                    responseBuilder.setAllowed(true);
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }
        });
    }
}
