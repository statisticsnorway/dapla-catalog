package no.ssb.dapla.catalog.dataset;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.catalog.UserAccessClient;
import no.ssb.testing.helidon.MockRegistry;

import java.util.Set;

public class CatalogMockRegistry extends MockRegistry {

    private static final Set<String> ACCESS = Set.of("a-user");

    public CatalogMockRegistry() {
        add((UserAccessClient) (userId, privilege, path, valuation, state, jwtToken) -> Single.just(ACCESS.contains(userId)));
    }
}
