package no.ssb.dapla.catalog.dataset;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import io.helidon.config.Config;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.PubSubAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetUpstreamGooglePubSubIntegrationInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetUpstreamGooglePubSubIntegrationInitializer.class);

    public static void initializeTopicsAndSubscriptions(Config pubSubUpstreamConfig, PubSub pubSub) {
        String projectId = pubSubUpstreamConfig.get("projectId").asString().get();
        String topicName = pubSubUpstreamConfig.get("topic").asString().get();
        String subscriptionName = pubSubUpstreamConfig.get("subscription").asString().get();
        try (TopicAdminClient topicAdminClient = pubSub.getTopicAdminClient()) {
            LOG.info("Initializing topic: {}", topicName);
            PubSubAdmin.createTopicIfNotExists(topicAdminClient, projectId, topicName);
            try (SubscriptionAdminClient subscriptionAdminClient = pubSub.getSubscriptionAdminClient()) {
                LOG.info("Initializing subscription: {}", subscriptionName);
                PubSubAdmin.createSubscriptionIfNotExists(subscriptionAdminClient, projectId, topicName, subscriptionName, 60);
            }
        }
    }
}
