package no.ssb.dapla.catalog.dataset;

import io.helidon.common.http.Http;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static no.ssb.dapla.catalog.dataset.Tracing.logError;
import static no.ssb.dapla.catalog.dataset.Tracing.spanFromHttp;

public class NameHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(NameHttpService.class);

    final NameIndex nameIndex;

    public NameHttpService(NameIndex nameIndex) {
        this.nameIndex = nameIndex;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get(this::mapNameToId);
        rules.post(Handler.create(MapNameToIdRequest.class, this::mapNameToIdWithProposedId));
        rules.delete(this::deleteNameToIdMapping);
    }

    private String getNamePathParamWhenNotMappedAsRoutingPattern(ServerRequest httpRequest) {
        String namePathParam = URLDecoder.decode(httpRequest.path().toRawString(), StandardCharsets.UTF_8);
        while (namePathParam.startsWith("/")) {
            namePathParam = namePathParam.substring(1);
        }
        namePathParam = "/" + namePathParam;
        return NamespaceUtils.toNamespace(NamespaceUtils.toComponents(namePathParam));
    }

    public void mapNameToId(ServerRequest httpRequest, ServerResponse httpResponse) {
        Span span = spanFromHttp(httpRequest, "mapNameToId");
        try {
            String name = getNamePathParamWhenNotMappedAsRoutingPattern(httpRequest);
            nameIndex.mapNameToId(name)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(datasetId -> {
                        if (datasetId == null) {
                            httpResponse.status(Http.Status.NOT_FOUND_404).send();
                            span.finish();
                            return;
                        }
                        MapNameToIdResponse response = MapNameToIdResponse.newBuilder().setId(datasetId).build();
                        httpResponse.send(response);
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            logError(span, throwable, "error in nameIndex.mapNameToId(name)");
                            LOG.error(String.format("error in nameIndex.mapNameToId(name): name='%s'", name), throwable);
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

    public void mapNameToIdWithProposedId(ServerRequest httpRequest, ServerResponse httpResponse, MapNameToIdRequest mapNameToIdRequest) {
        Span span = spanFromHttp(httpRequest, "mapNameToIdWithProposedId");
        try {
            String name = getNamePathParamWhenNotMappedAsRoutingPattern(httpRequest);
            String proposedId = mapNameToIdRequest.getProposedId().isBlank() ? UUID.randomUUID().toString() : mapNameToIdRequest.getProposedId();
            nameIndex.mapNameToId(name, proposedId)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(datasetId -> {
                        if (datasetId == null) {
                            httpResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send("Unable to map name to the proposedId.");
                            span.finish();
                            return;
                        }
                        MapNameToIdResponse response = MapNameToIdResponse.newBuilder().setId(datasetId).build();
                        httpResponse.send(response);
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            logError(span, throwable, "error in nameIndex.mapNameToId(name, proposedId)");
                            LOG.error(String.format("error in nameIndex.mapNameToId(name, proposedId): name='%s', proposedId='%s'", name, proposedId), throwable);
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

    private void deleteNameToIdMapping(ServerRequest serverRequest, ServerResponse serverResponse) {
        Span span = spanFromHttp(serverRequest, "deleteNameToIdMapping");
        try {
            String name = getNamePathParamWhenNotMappedAsRoutingPattern(serverRequest);
            nameIndex.deleteMappingFor(name)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(v -> {
                        serverResponse.send();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            logError(span, throwable, "error in nameIndex.deleteMappingFor(name)");
                            LOG.error(String.format("While serving deleteNameToIdMapping for name: %s", name), throwable);
                            serverResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(throwable.getMessage());
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
