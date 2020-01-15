package no.ssb.dapla.catalog.dataset;

import io.helidon.common.http.Http;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class NameService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(NameService.class);

    final NameIndex nameIndex;

    public NameService(NameIndex nameIndex) {
        this.nameIndex = nameIndex;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{name}", this::httpGetMapNameToId);
        rules.post("/{name}/{proposedId}", this::httpPostMapNameToId);
        rules.delete("/{name}", this::httpDeleteMapNameToId);
    }

    public void httpGetMapNameToId(ServerRequest httpRequest, ServerResponse httpResponse) {
        String name = httpRequest.path().param("name");
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

    public void httpPostMapNameToId(ServerRequest httpRequest, ServerResponse httpResponse) {
        String name = httpRequest.path().param("name");
        String proposedId = httpRequest.path().param("proposedId");
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
        String name = serverRequest.path().param("name");
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
