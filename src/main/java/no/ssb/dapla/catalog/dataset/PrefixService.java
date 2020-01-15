package no.ssb.dapla.catalog.dataset;

import io.helidon.common.http.Http;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PrefixService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(PrefixService.class);

    final NameIndex nameIndex;

    public PrefixService(NameIndex nameIndex) {
        this.nameIndex = nameIndex;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get(this::httpListByPrefix);
    }

    public void httpListByPrefix(ServerRequest httpRequest, ServerResponse httpResponse) {
        String prefix = httpRequest.path().toString();
        int limit = Integer.parseInt(httpRequest.queryParams().first("limit").orElse("100"));
        nameIndex.listByPrefix(prefix, limit)
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(entries -> {
                    if (entries == null || entries.isEmpty()) {
                        httpResponse.status(Http.Status.NOT_FOUND_404).send();
                        return;
                    }
                    ListByPrefixResponse responseEntity = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                    httpResponse.send(responseEntity);
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving httpListByPrefix for prefix: %s", prefix), throwable);
                    httpResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(throwable.getMessage());
                    return null;
                })
        ;
    }
}
