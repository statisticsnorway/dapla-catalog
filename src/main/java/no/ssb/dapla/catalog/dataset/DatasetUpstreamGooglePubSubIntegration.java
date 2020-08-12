package no.ssb.dapla.catalog.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import io.helidon.config.Config;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DatasetUpstreamGooglePubSubIntegration implements MessageReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetUpstreamGooglePubSubIntegration.class);

    final PubSub pubSub;
    final DatasetRepository repository;
    final ObjectMapper objectMapper = new ObjectMapper();

    final Subscriber subscriber;

    public DatasetUpstreamGooglePubSubIntegration(Config pubSubUpstreamConfig, PubSub pubSub, DatasetRepository repository) {
        this.pubSub = pubSub;
        this.repository = repository;

        String projectId = pubSubUpstreamConfig.get("projectId").asString().get();
        String topicName = pubSubUpstreamConfig.get("topic").asString().get();
        String subscriptionName = pubSubUpstreamConfig.get("subscription").asString().get();

        LOG.info("Using upstream topic: {}", topicName);
        LOG.info("Using upstream subscription: {}", subscriptionName);
        LOG.info("Creating subscriber");
        subscriber = pubSub.getSubscriber(projectId, subscriptionName, this);
        subscriber.addListener(
                new Subscriber.Listener() {
                    public void failed(Subscriber.State from, Throwable failure) {
                        LOG.error(String.format("Error with subscriber on subscription '%s' and topic '%s'", subscriptionName, topicName), failure);
                    }
                },
                MoreExecutors.directExecutor());
        LOG.info("Subscriber async pull starting...");
        subscriber.startAsync().awaitRunning();
        LOG.info("Subscriber async pull is now running.");
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        try {
            JsonNode dataNode;
            try (InputStream inputStream = message.getData().newInput()) {
                dataNode = objectMapper.readTree(inputStream);
            }
            if (!dataNode.has("parentUri")
                    || !dataNode.has("dataset-meta")) {
                LOG.warn("Message IGNORED. Received message with invalid protocol. Missing 'parentUri' and/or 'dataset-meta' fields in json-document.");
                consumer.ack();
                return;
            }
            String parentUri = dataNode.get("parentUri").textValue();
            JsonNode datasetMetaNode = dataNode.get("dataset-meta");
            String metadataJson = objectMapper.writeValueAsString(datasetMetaNode);
            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(metadataJson, DatasetMeta.class);
            Dataset dataset = Dataset.newBuilder()
                    .setId(DatasetId.newBuilder()
                            .setPath(datasetMeta.getId().getPath())
                            .setTimestamp(Long.parseLong(datasetMeta.getId().getVersion()))
                            .build())
                    .setType(Dataset.Type.valueOf(datasetMeta.getType().name()))
                    .setValuation(Dataset.Valuation.valueOf(datasetMeta.getValuation().name()))
                    .setState(Dataset.DatasetState.valueOf(datasetMeta.getState().name()))
                    .setParentUri(parentUri)
                    .setPseudoConfig(PseudoConfig.parseFrom(datasetMeta.getPseudoConfig().toByteString())) // use serialization to cast, assume they are compatible
                    .build();
            repository.create(dataset)
                    .doOnSuccess(rowsUpdated -> {
                        repository.setPathDirty(datasetMeta.getId().getPath(), Dataset.IsDirty.CLEAN);
                        consumer.ack();
                        LOG.trace("Saved Dataset. json='{}'", ProtobufJsonUtils.toString(dataset));
                    })
                    .doOnError(throwable -> LOG.error("Error while processing message, waiting for ack deadline before re-delivery", throwable))
                    .subscribe();
        } catch (Throwable t) {
            LOG.error("Error while processing message, waiting for ack deadline before re-delivery", t);
        }
    }

    public void close() {
        subscriber.stopAsync();
        try {
            subscriber.awaitTerminated(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
