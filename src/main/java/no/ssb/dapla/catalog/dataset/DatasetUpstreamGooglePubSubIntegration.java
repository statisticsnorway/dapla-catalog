package no.ssb.dapla.catalog.dataset;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.helidon.config.Config;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DatasetUpstreamGooglePubSubIntegration implements MessageReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetUpstreamGooglePubSubIntegration.class);

    final TransportChannelProvider channelProvider;
    final CredentialsProvider credentialsProvider;
    final DatasetRepository repository;

    final Subscriber subscriber;

    public DatasetUpstreamGooglePubSubIntegration(Config pubSubUpstreamConfig, TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider, DatasetRepository repository) {
        this.channelProvider = channelProvider;
        this.credentialsProvider = credentialsProvider;
        this.repository = repository;

        String projectId = pubSubUpstreamConfig.get("projectId").asString().get();
        ProjectName projectName = ProjectName.of(projectId);
        String topicName = pubSubUpstreamConfig.get("topic").asString().get();
        String subscriptionName = pubSubUpstreamConfig.get("subscription").asString().get();
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
        ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);

        try (TopicAdminClient topicAdminClient = getTopicAdminClient()) {
            if (!topicExists(topicAdminClient, projectName, projectTopicName)) {
                topicAdminClient.createTopic(projectTopicName);
            }
            try (SubscriptionAdminClient subscriptionAdminClient = getSubscriptionAdminClient()) {
                if (!subscriptionExists(subscriptionAdminClient, projectName, projectSubscriptionName)) {
                    subscriptionAdminClient.createSubscription(projectSubscriptionName, projectTopicName, PushConfig.getDefaultInstance(), 10);
                }
                subscriber = Subscriber.newBuilder(projectSubscriptionName, this)
                        .setChannelProvider(this.channelProvider)
                        .setCredentialsProvider(this.credentialsProvider)
                        .build();
                subscriber.addListener(
                        new Subscriber.Listener() {
                            public void failed(Subscriber.State from, Throwable failure) {
                                LOG.error(String.format("Error with subscriber on subscription: '%s'", projectSubscriptionName), failure);
                            }
                        },
                        MoreExecutors.directExecutor());
                subscriber.startAsync().awaitRunning();
            }
        }
    }

    private boolean topicExists(TopicAdminClient topicAdminClient, ProjectName projectName, ProjectTopicName projectTopicName) {
        final int PAGE_SIZE = 25;
        TopicAdminClient.ListTopicsPagedResponse listResponse = topicAdminClient.listTopics(ListTopicsRequest.newBuilder().setProject(projectName.toString()).setPageSize(PAGE_SIZE).build());
        for (Topic topic : listResponse.iterateAll()) {
            if (topic.getName().equals(projectTopicName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = topicAdminClient.listTopics(ListTopicsRequest.newBuilder().setProject(projectName.toString()).setPageToken(listResponse.getNextPageToken()).setPageSize(PAGE_SIZE).build());
            for (Topic topic : listResponse.iterateAll()) {
                if (topic.getName().equals(projectTopicName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean subscriptionExists(SubscriptionAdminClient subscriptionAdminClient, ProjectName projectName, ProjectSubscriptionName projectSubscriptionName) {
        final int PAGE_SIZE = 25;
        SubscriptionAdminClient.ListSubscriptionsPagedResponse listResponse = subscriptionAdminClient.listSubscriptions(ListSubscriptionsRequest.newBuilder().setProject(projectName.toString()).setPageSize(PAGE_SIZE).build());
        for (Subscription subscription : listResponse.iterateAll()) {
            if (subscription.getName().equals(projectSubscriptionName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = subscriptionAdminClient.listSubscriptions(ListSubscriptionsRequest.newBuilder().setProject(projectName.toString()).setPageToken(listResponse.getNextPageToken()).setPageSize(PAGE_SIZE).build());
            for (Subscription subscription : listResponse.iterateAll()) {
                if (subscription.getName().equals(projectSubscriptionName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    private TopicAdminClient getTopicAdminClient() {
        try {
            return TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SubscriptionAdminClient getSubscriptionAdminClient() {
        try {
            return SubscriptionAdminClient.create(
                    SubscriptionAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        try {
            TransportChannel channel = channelProvider.getTransportChannel();
            channel.shutdown();
            if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                channel.shutdownNow();
                if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Unable to close channel");
                }
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
