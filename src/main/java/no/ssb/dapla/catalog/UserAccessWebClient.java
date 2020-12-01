package no.ssb.dapla.catalog;

import io.helidon.common.reactive.Single;
import io.helidon.webclient.WebClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class UserAccessWebClient implements UserAccessClient {

    private final WebClient webClient;

    public UserAccessWebClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public UserAccessWebClient(URI baseUri) {
        this.webClient = WebClient.builder()
                .baseUri(baseUri)
                .build();
    }

    public UserAccessWebClient(String host, int port) {
        try {
            this.webClient = WebClient.builder()
                    .baseUri(new URI("http", null, host, port, null, null, null))
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public Single<Boolean> hasAccess(String userId, String privilege, String path, String valuation, String state, String jwtToken) {
        return webClient.get()
                .path("/access/" + URLEncoder.encode(userId, StandardCharsets.UTF_8))
                .queryParam("privilege", privilege)
                .queryParam("path", path)
                .queryParam("valuation", valuation)
                .queryParam("state", state)
                .headers(headers -> {
                    headers.put("Authorization", "Bearer: " + jwtToken);
                    return headers;
                })
                .request()
                .map(wcr -> wcr.status().code() == 200);
    }
}
