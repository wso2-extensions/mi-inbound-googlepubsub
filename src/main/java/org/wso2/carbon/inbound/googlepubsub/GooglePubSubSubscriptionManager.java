package org.wso2.carbon.inbound.googlepubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.RetryPolicy;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.UpdateSubscriptionRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GooglePubSubSubscriptionManager {
    private static final Log log = LogFactory.getLog(GooglePubSubSubscriptionManager.class);
    private final String projectId;
    private final String topicId;
    private final String subscriptionId;
    private final String filter;
    private final int ackDeadlineSeconds;
    private final DeadLetterPolicy deadLetterPolicy;
    private final boolean updateSubscriptionIfExists;
    private final boolean exactlyOnceDelivery;
    private final long messageRetentionDuration;
    private final RetryPolicy retryPolicy;
    private final Map<String, String> labels;
    private boolean enableMessageOrdering;
    private boolean retainAckedMessages;
    private SubscriptionAdminSettings adminSettings;

    public GooglePubSubSubscriptionManager(String projectId, String topicId, String subscriptionId, String filter,
            int ackDeadlineSeconds, DeadLetterPolicy deadLetterPolicy, boolean updateSubscriptionIfExists,
            boolean exactlyOnceDelivery, long messageRetentionDuration, RetryPolicy retryPolicy, String labelsList,
            GoogleCredentials credentials, boolean enableMessageOrdering, boolean retainAckedMessages)
            throws IOException {
        this.projectId = projectId;
        this.topicId = topicId;
        this.subscriptionId = subscriptionId;
        this.filter = filter;
        this.ackDeadlineSeconds = ackDeadlineSeconds;
        this.deadLetterPolicy = deadLetterPolicy;
        this.updateSubscriptionIfExists = updateSubscriptionIfExists;
        this.exactlyOnceDelivery = exactlyOnceDelivery;
        this.messageRetentionDuration = messageRetentionDuration;
        this.retryPolicy = retryPolicy;
        this.retainAckedMessages = retainAckedMessages;
        this.labels = parseLabels(labelsList);
        this.enableMessageOrdering = enableMessageOrdering;
        this.adminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
    }

    public void createSubscriptionIfNotExists() {
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        try (SubscriptionAdminClient client = SubscriptionAdminClient.create(adminSettings)) {
            Subscription.Builder subscriptionBuilder = Subscription.newBuilder().setName(subscriptionName.toString())
                    .setTopic(topicName.toString()).setFilter(filter).setRetryPolicy(retryPolicy)
                    .setMessageRetentionDuration(Duration.newBuilder().setSeconds(messageRetentionDuration * 60L))
                    .setAckDeadlineSeconds(ackDeadlineSeconds).setEnableMessageOrdering(enableMessageOrdering)
                    .setEnableExactlyOnceDelivery(exactlyOnceDelivery).setRetainAckedMessages(retainAckedMessages)
                    .putAllLabels(labels);

            if (deadLetterPolicy != null)
                subscriptionBuilder.setDeadLetterPolicy(deadLetterPolicy);

            client.createSubscription(subscriptionBuilder.build());
            log.info(
                    "Subscription " + subscriptionId + " Created. Subscription Details: " + subscriptionBuilder.getAllFields());
        } catch (AlreadyExistsException e) {
            log.warn("Subscription already exists: " + subscriptionId);
            checkAndUpdateSubscription(subscriptionName);
        } catch (PermissionDeniedException e) {
            log.error("You don't have permission to create subscription " + e);
        } catch (Exception e) {
            log.error("Error on creating subscription " + e);
        }
    }

    public void checkAndUpdateSubscription(ProjectSubscriptionName subscriptionName) {
        try (SubscriptionAdminClient client = SubscriptionAdminClient.create(adminSettings)) {
            Subscription existingSubscription = client.getSubscription(subscriptionName);
            if (existingSubscription.getState() == Subscription.State.RESOURCE_ERROR) {
                deleteSubscription(projectId, subscriptionId);
                createSubscriptionIfNotExists();
            }
            if (checkStaticAndUpdate(existingSubscription, subscriptionName))
                return;
            checkAndApplyDynamicUpdates(existingSubscription, subscriptionName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkStaticAndUpdate(Subscription sub, ProjectSubscriptionName name) {
        if (!Objects.equals(sub.getTopic(), ProjectTopicName.format(name.getProject(), topicId))) {
            log.error("Same subscription name exists for a different Topic: " + sub.getTopic());
            throw new IllegalStateException("Same subscription name exists for a different Topic.");
        }
        if (!Objects.equals(sub.getEnableMessageOrdering(), enableMessageOrdering) || !Objects.equals(sub.getFilter(),
                filter)) {
            log.warn("Filter or message ordering properties mismatch. Recreating subscription.");
            if (updateSubscriptionIfExists) {
                deleteSubscription(projectId, subscriptionId);
                createSubscriptionIfNotExists();
                return true;
            } else {
                throw new IllegalStateException("Static property mismatch");
            }
        }
        return false;
    }

    public void deleteSubscription(String projectId, String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(adminSettings)) {
            subscriptionAdminClient.deleteSubscription(subscriptionName);
        } catch (Exception e) {
            throw new RuntimeException("Error deleting subscription " + e);
        }
    }

    private void checkAndApplyDynamicUpdates(Subscription sub, ProjectSubscriptionName name) throws IOException {
        Subscription.Builder builder = Subscription.newBuilder().setName(name.toString());
        boolean updated = false;
        FieldMask.Builder updateMask = FieldMask.newBuilder();

        updated |= compareAndSet(sub.getAckDeadlineSeconds(), ackDeadlineSeconds,
                GooglePubSubConstants.PROP_ACK_DEADLINE_SECONDS, builder::setAckDeadlineSeconds, updateMask);
        updated |= compareAndSet(sub.getRetainAckedMessages(), retainAckedMessages,
                GooglePubSubConstants.PROP_RETAIN_ACKED_MESSAGES, builder::setRetainAckedMessages, updateMask);

        updated |= compareAndSet(sub.getMessageRetentionDuration().getSeconds(), messageRetentionDuration * 60L,
                GooglePubSubConstants.PROP_MESSAGE_RETENTION_DURATION,
                val -> builder.setMessageRetentionDuration(Duration.newBuilder().setSeconds(val)), updateMask);

        if (deadLetterPolicy != null) {
            DeadLetterPolicy subPolicy = sub.getDeadLetterPolicy();
            updated |= compareAndSet(subPolicy.getDeadLetterTopic(), deadLetterPolicy.getDeadLetterTopic(),
                    GooglePubSubConstants.PROP_DLP_TOPIC, val -> builder.setDeadLetterPolicy(
                            DeadLetterPolicy.newBuilder().setDeadLetterTopic(val)
                                    .setMaxDeliveryAttempts(deadLetterPolicy.getMaxDeliveryAttempts()).build()),
                    updateMask);
            updated |= compareAndSet(subPolicy.getMaxDeliveryAttempts(), deadLetterPolicy.getMaxDeliveryAttempts(),
                    GooglePubSubConstants.PROP_DLP_DELIVERY_ATTEMPTS, val -> builder.setDeadLetterPolicy(
                            DeadLetterPolicy.newBuilder().setMaxDeliveryAttempts(val)
                                    .setDeadLetterTopic(deadLetterPolicy.getDeadLetterTopic()).build()), updateMask);
        }
        if (retryPolicy != null) {
            RetryPolicy subRetry = sub.getRetryPolicy();
            updated |= compareAndSet(subRetry.getMinimumBackoff().getSeconds(),
                    retryPolicy.getMinimumBackoff().getSeconds(), GooglePubSubConstants.PROP_RP_MIN_BACKOFF,
                    val -> builder.setRetryPolicy(
                            RetryPolicy.newBuilder().setMinimumBackoff(Duration.newBuilder().setSeconds(val)).build()),
                    updateMask);
            updated |= compareAndSet(subRetry.getMaximumBackoff().getSeconds(),
                    retryPolicy.getMaximumBackoff().getSeconds(), GooglePubSubConstants.PROP_RP_MAX_BACKOFF,
                    val -> builder.setRetryPolicy(
                            RetryPolicy.newBuilder().setMaximumBackoff(Duration.newBuilder().setSeconds(val)).build()),
                    updateMask);
        }

        if (!labels.equals(sub.getLabelsMap())) {
            builder.putAllLabels(labels);
            updateMask.addPaths(GooglePubSubConstants.LABELS);
            updated = true;
        }
        if (updated) {
            log.info("Subscription properties need update.");
            if (updateSubscriptionIfExists) {
                log.info("Allowed to update the existing subscription properties hence updating...");
                try (SubscriptionAdminClient client = SubscriptionAdminClient.create(adminSettings)) {
                    UpdateSubscriptionRequest req = UpdateSubscriptionRequest.newBuilder()
                            .setSubscription(builder.build()).setUpdateMask(updateMask.build()).build();
                    client.updateSubscription(req);
                    log.info("Subscription updated: " + name);
                }
            } else {
                log.info("Updating not allowed");
                throw new IllegalStateException(
                        "Existing subscription properties mismatch, but update is not enabled. Try enabling updateSubscriptionIfExists");
            }
        } else {
            log.info("No subscription updates required and existing can be used.");
        }
    }

    private <T> boolean compareAndSet(T actual, T expected, String fieldName, java.util.function.Consumer<T> setter,
            FieldMask.Builder mask) {
        if (!Objects.equals(actual, expected)) {
            log.warn(String.format(
                    "Subscription Property mismatch. For current subscription, field '%s' value is  [%s] but new value is [%s]",
                    fieldName, actual, expected));
            setter.accept(expected);
            mask.addPaths(fieldName);
            return true;
        }
        return false;
    }

    public Map<String, String> parseLabels(String input) {
        Map<String, String> labelMap = new HashMap<>();
        if (input == null || input.isEmpty()) {
            return labelMap;
        }
        String[] pairs = input.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                labelMap.put(kv[0].trim(), kv[1].trim());
            }
        }
        return labelMap;
    }
}
