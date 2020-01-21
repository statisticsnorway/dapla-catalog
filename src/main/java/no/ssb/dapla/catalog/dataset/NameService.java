package no.ssb.dapla.catalog.dataset;

import io.helidon.common.http.Http;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.helidon.application.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class NameService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(NameService.class);

    final NameIndex nameIndex;

    public NameService(NameIndex nameIndex) {
        this.nameIndex = nameIndex;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get(this::httpGetMapNameToId);
        rules.post(Handler.create(MapNameToIdRequest.class, this::httpPostMapNameToId));
        rules.delete(this::httpDeleteMapNameToId);
    }

    private String getNamePathParamWhenNotMappedAsRoutingPattern(ServerRequest httpRequest) {
        String namePathParam = URLDecoder.decode(httpRequest.path().toRawString(), StandardCharsets.UTF_8);
        while (namePathParam.startsWith("/")) {
            namePathParam = namePathParam.substring(1);
        }
        namePathParam = "/" + namePathParam;
        return NamespaceUtils.toNamespace(NamespaceUtils.toComponents(namePathParam));
    }

    public void httpGetMapNameToId(ServerRequest httpRequest, ServerResponse httpResponse) {
        LogUtils.trace(LOG, "httpGetMapNameToId", httpRequest);
        String name = getNamePathParamWhenNotMappedAsRoutingPattern(httpRequest);
        nameIndex.mapNameToId(name)
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(datasetId -> {
                    if (datasetId == null) {
                        httpResponse.status(Http.Status.NOT_FOUND_404).send();
                        return;
                    }
                    MapNameToIdResponse response = MapNameToIdResponse.newBuilder().setId(datasetId).build();
                    httpResponse.send(response);
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving httpGetMapNameToId for name: %s", name), throwable);
                    httpResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(throwable.getMessage());
                    return null;
                })
        ;
    }

    public void httpPostMapNameToId(ServerRequest httpRequest, ServerResponse httpResponse, MapNameToIdRequest mapNameToIdRequest) {
        LogUtils.trace(LOG, "httpPostMapNameToId", httpRequest, mapNameToIdRequest);
        String name = getNamePathParamWhenNotMappedAsRoutingPattern(httpRequest);
        String proposedId = mapNameToIdRequest.getProposedId().isBlank() ? UUID.randomUUID().toString() : mapNameToIdRequest.getProposedId();
        nameIndex.mapNameToId(name, proposedId)
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(datasetId -> {
                    if (datasetId == null) {
                        httpResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send("Unable to map name to the proposedId.");
                        return;
                    }
                    MapNameToIdResponse response = MapNameToIdResponse.newBuilder().setId(datasetId).build();
                    httpResponse.send(response);
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving httpPostMapNameToId for name: %s", name), throwable);
                    httpResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(throwable.getMessage());
                    return null;
                })
        ;
    }

    private void httpDeleteMapNameToId(ServerRequest serverRequest, ServerResponse serverResponse) {
        LogUtils.trace(LOG, "httpDeleteMapNameToId", serverRequest);
        String name = getNamePathParamWhenNotMappedAsRoutingPattern(serverRequest);
        nameIndex.deleteMappingFor(name)
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(v -> {
                    serverResponse.send();
                })
                .exceptionally(throwable -> {
                    LOG.error(String.format("While serving httpPostMapNameToId for name: %s", name), throwable);
                    serverResponse.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(throwable.getMessage());
                    return null;
                })
        ;
    }
}
