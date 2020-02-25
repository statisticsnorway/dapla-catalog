package no.ssb.dapla.catalog.dataset;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import io.helidon.config.Config;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DatasetUpstreamGooglePubSubIntegration implements MessageReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetUpstreamGooglePubSubIntegration.class);

    final PubSub pubSub;
    final DatasetRepository repository;

    final Subscriber subscriber;

    public DatasetUpstreamGooglePubSubIntegration(Config pubSubUpstreamConfig, PubSub pubSub, DatasetRepository repository) {
        this.pubSub = pubSub;
        this.repository = repository;

        String projectId = pubSubUpstreamConfig.get("projectId").asString().get();
        ProjectName projectName = ProjectName.of(projectId);
        String topicName = pubSubUpstreamConfig.get("topic").asString().get();
        String subscriptionName = pubSubUpstreamConfig.get("subscription").asString().get();
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
        ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);

        try (TopicAdminClient topicAdminClient = pubSub.getTopicAdminClient()) {
            if (!pubSub.topicExists(topicAdminClient, projectName, projectTopicName, 25)) {
                topicAdminClient.createTopic(projectTopicName);
            }
            try (SubscriptionAdminClient subscriptionAdminClient = pubSub.getSubscriptionAdminClient()) {
                if (!pubSub.subscriptionExists(subscriptionAdminClient, projectName, projectSubscriptionName, 25)) {
                    subscriptionAdminClient.createSubscription(projectSubscriptionName, projectTopicName, PushConfig.getDefaultInstance(), 10);
                }
                subscriber = pubSub.getSubscriber(projectSubscriptionName, this);
                subscriber.addListener(
                        new Subscriber.Listener() {
                            public void failed(Subscriber.State from, Throwable failure) {
                                LOG.error(String.format("Error with subscriber on subscription '%s' and topic '%s'", projectSubscriptionName, projectTopicName), failure);
                            }
                        },
                        MoreExecutors.directExecutor());
                subscriber.startAsync().awaitRunning();
            }
        }
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        try {
            DatasetMeta datasetMeta = DatasetMeta.parseFrom(message.getData());
            repository.create(Dataset.newBuilder()
                    .setId(DatasetId.newBuilder()
                            .setPath(datasetMeta.getId().getPath())
                            .setTimestamp(datasetMeta.getId().getVersion())
                            .build())
                    .setType(Dataset.Type.valueOf(datasetMeta.getType().name()))
                    .setValuation(Dataset.Valuation.valueOf(datasetMeta.getValuation().name()))
                    .setState(Dataset.DatasetState.valueOf(datasetMeta.getState().name()))
                    .setParentUri(datasetMeta.getParentUri())
                    .setPseudoConfig(PseudoConfig.parseFrom(datasetMeta.getPseudoConfig().toByteString())) // use serialization to cast, assume they are compatible
                    .build())
                    .blockingGet();
            LOG.trace("Saved DatasetMeta. json='{}'", ProtobufJsonUtils.toString(datasetMeta));
            consumer.ack();
        } catch (RuntimeException | Error e) {
            LOG.error("Sending nack on message to force re-delivery", e);
            consumer.nack();
            throw e;
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Sending nack on message to force re-delivery", e);
            consumer.nack();
            throw new RuntimeException(e);
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
