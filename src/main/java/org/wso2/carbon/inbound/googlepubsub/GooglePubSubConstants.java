/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.inbound.googlepubsub;

public final class GooglePubSubConstants {

    public static final String PROJECT_ID = "projectId";
    public static final String TOPIC_ID = "topicId";
    public static final String SUBSCRIPTION_ID = "subscriptionId";
    public static final String CONTENT_TYPE = "contentType";
    public static final String MESSAGE_FILTER = "messageFilter";
    public static final String ACK_DEADLINE_SECONDS = "ackDeadlineSeconds";
    public static final String DEAD_LETTER_TOPIC = "deadLetterTopic";
    public static final String MAX_DELIVERY_ATTEMPTS = "maxDeliveryAttempts";
    public static final String CREATE_SUBSCRIPTION_ON_CONNECT = "createSubscriptionOnConnect";
    public static final String MIN_BACKOFF_DURATION = "minBackOffDuration";
    public static final String MAX_BACKOFF_DURATION = "maxBackOffDuration";
    public static final String LABELS = "labels";
    public static final String UPDATE_SUBSCRIPTION_IF_EXISTS = "updateSubscriptionIfExists";
    public static final String ENABLE_MESSAGE_ORDERING = "enableMessageOrdering";
    public static final String KEY_FILE_PATH = "keyFilePath";
    public static final String MAX_MESSAGE_COUNT = "maxOutstandingMessageCount";
    public static final String MAX_MESSAGE_SIZE = "maxOutstandingMessageSize";
    public static final String MESSAGE_RETENTION_DURATION = "messageRetentionDuration";
    public static final String EXACTLY_ONCE_DELIVERY = "exactlyOnceDelivery";
    public static final String EXECUTOR_THREADS_PER_CONSUMERS = "executorThreadsPerConsumers";
    public static final String CONCURRENT_CONSUMERS = "concurrentConsumers";
    public static final String ENDPOINT = "endpoint";
    public static final String RETAIN_ACKED_MESSAGES = "retainAckedMessages";
    public static final String SET_ROLLBACK_ONLY = "SET_ROLLBACK_ONLY";
    public static final String GOOGLE_PUBSUB_CONSUMER_THREAD_NAME = "googlepubsub-subscriber-pool-";
    public static final String GOOGLE_CLIENT_DELIVERY_ATTEMPT ="googclient_deliveryattempt";

    /**
     * Record Details
     */
    public static final String MESSAGE_ID = "MessageId";
    public static final String MESSAGE_BODY = "MessageBody";
    public static final String MESSAGE_PUBLISHED_TIME = "MessagePublishTime";
    public static final String MESSAGE_ORDERING_KEY = "MessageOrderingKey";

    /**
     * Subscription Properties
     */
    public static final String PROP_ACK_DEADLINE_SECONDS = "ack_deadline_seconds";
    public static final String PROP_RETAIN_ACKED_MESSAGES = "retain_acked_messages";
    public static final String PROP_MESSAGE_RETENTION_DURATION = "message_retention_duration";
    public static final String PROP_DLP_TOPIC = "dead_letter_policy.dead_letter_topic";
    public static final String PROP_DLP_DELIVERY_ATTEMPTS = "dead_letter_policy.max_delivery_attempts";
    public static final String PROP_RP_MIN_BACKOFF = "minimum_backoff";
    public static final String PROP_RP_MAX_BACKOFF = "maximum_backoff";
    /**
     * Default Values
     */

    public static final String DEFAULT_CONTENT_TYPE = "application/json";
    public static final String DEFAULT_EXECUTOR_THREADS_PER_CONSUMERS = "5";
    public static final String DEFAULT_CONCURRENT_CONSUMERS = "1";
    public static final String DEFAULT_ENDPOINT = "pubsub.googleapis.com:443";
    public static final String DEFAULT_ACK_DEADLINE_SECONDS = "10";
    public static final String DEFAULT_MAX_MESSAGE_COUNT = "1000";
    public static final String DEFAULT_MAX_MESSAGE_SIZE_IN_MB = "100";
    public static final String DEFAULT_MAX_DELIVERY_ATTEMPTS = "5";
    public static final String DEFAULT_MESSAGE_RETENTION_DURATION = "10080";
    public static final String DEFAULT_MIN_BACKOFF_DURATION = "10";
    public static final String DEFAULT_MAX_BACKOFF_DURATION = "600";
}
