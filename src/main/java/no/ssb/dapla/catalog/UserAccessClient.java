package no.ssb.dapla.catalog;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.Privilege;
import no.ssb.dapla.dataset.api.DatasetMeta;

public interface UserAccessClient {

    default Single<AccessCheckResponse> hasAccess(AccessCheckRequest request, String jwtToken) {
        return hasAccess(request.getUserId(), request.getPrivilege(), request.getPath(), request.getValuation(), request.getState(), jwtToken)
                .map(access -> AccessCheckResponse.newBuilder()
                        .setAllowed(access)
                        .build());
    }

    default Single<Boolean> hasAccess(String userId, Privilege privilege, DatasetMeta datasetMeta, String jwtToken) {
        return hasAccess(userId, privilege.name(), datasetMeta.getId().getPath(), datasetMeta.getValuation().name(), datasetMeta.getState().name(), jwtToken);
    }

    Single<Boolean> hasAccess(String userId, String privilege, String path, String valuation, String state, String jwtToken);
}
