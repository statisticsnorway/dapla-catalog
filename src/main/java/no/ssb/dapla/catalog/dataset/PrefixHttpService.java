package no.ssb.dapla.catalog.dataset;

import io.helidon.common.http.Http;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.dapla.catalog.dataset.Tracing.logError;
import static no.ssb.dapla.catalog.dataset.Tracing.spanFromHttp;

public class PrefixHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(PrefixHttpService.class);

    final NameIndex nameIndex;

    public PrefixHttpService(NameIndex nameIndex) {
        this.nameIndex = nameIndex;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get(this::listByPrefix);
    }

    public void listByPrefix(ServerRequest httpRequest, ServerResponse httpResponse) {
        Span span = spanFromHttp(httpRequest, "listByPrefix");
        try {
            String prefix = httpRequest.path().toString();
            int limit = Integer.parseInt(httpRequest.queryParams().first("limit").orElse("100"));
            nameIndex.listByPrefix(prefix, limit)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(entries -> {
                        if (entries == null || entries.isEmpty()) {
                            httpResponse.status(Http.Status.NOT_FOUND_404).send();
                            span.finish();
                            return;
                        }
                        ListByPrefixResponse responseEntity = ListByPrefixResponse.newBuilder().addAllEntries(entries).build();
                        httpResponse.send(responseEntity);
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            logError(span, throwable, "error in nameIndex.listByPrefix()");
                            LOG.error(String.format("While serving httpListByPrefix for prefix: %s", prefix), throwable);
                            httpResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(throwable.getMessage());
                            return null;
                        } finally {
                            span.finish();
                        }
                    })
            ;
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }
}
