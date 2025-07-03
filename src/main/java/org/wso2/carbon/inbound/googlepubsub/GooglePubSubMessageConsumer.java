package org.wso2.carbon.inbound.googlepubsub;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.pubsub.v1.*;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class GooglePubSubMessageConsumer extends GenericPollingConsumer {
    private static final Log log = LogFactory.getLog(GooglePubSubMessageConsumer.class);
    private  String projectId;
    private  String contentType;
    private  String subscriptionId;
    private  GooglePubSubSubscriptionManager subscriptionManager;
    private Subscriber subscriber = null;
    private boolean isConsumed = false;
    private boolean isPolling = false;

    private long maxOutstandingMessageCount;
    private long maxOutstandingMessageSize;

    private static final AtomicInteger threadId = new AtomicInteger(1);

    private int concurrentConsumers;
    private String keyFilePath;
    private GoogleCredentials credentials;
    private boolean createSubscriptionOnConnect;
    private boolean updateSubscriptionIfExists;

    private int executorThreadsPerConsumers;
    private String endpoint;


    public GooglePubSubMessageConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment,
            long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);
        log.info("Initializing GooglePubSubMessageConsumer");
        loadConfiguration(properties);
        if (createSubscriptionOnConnect) {
            subscriptionManager.createSubscriptionIfNotExists();
        }
        if(updateSubscriptionIfExists && !createSubscriptionOnConnect){
            subscriptionManager.checkAndUpdateSubscription(ProjectSubscriptionName.of(projectId, subscriptionId));
        }
    }

    @Override
    public Object poll() {
        log.debug("Polling for messages from Google Pub/Sub...");
        if (subscriber != null) {
            isPolling = subscriber.isRunning();
            if (subscriber.state().equals("FAILED")) {
                log.info("Subscriber fails.. State: " + subscriber.state());
                subscriber = null;
            }
        }
        if (!isPolling && subscriber == null) {
            log.debug("Starting to consume messages from Google Pub/Sub");
            receiveMessages(projectId, subscriptionId);
        }
        return null;
    }

    @Override
    public void destroy() {
        log.info("Stopping the Google Pub/Sub Consumer");
        if (subscriber != null) {
            try {
                subscriber.stopAsync().awaitTerminated(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                log.info("Timed out while waiting for subscriber to terminate. Subscriber State:  " + subscriber.state());
            }
            subscriber=null;
            log.info("Google Pub/Sub Consumer Stopped ");
        }
    }

    private boolean injectMessage(String toStringUtf8, String contentType, MessageContext msgCtx) {
        AutoCloseInputStream in = new AutoCloseInputStream(new ByteArrayInputStream(toStringUtf8.getBytes()));
        log.debug("Thread ID: " + Thread.currentThread().getId() +
                " | Name: " + Thread.currentThread().getName());
        return this.injectMessage(in, contentType, msgCtx);
    }

    private MessageContext populateMessageContext(PubsubMessage message) {
        MessageContext msgCtx = createMessageContext();
        msgCtx.setProperty(GooglePubSubConstants.MESSAGE_ID, message.getMessageId());
        msgCtx.setProperty(GooglePubSubConstants.MESSAGE_BODY, message.getData());
        msgCtx.setProperty(GooglePubSubConstants.MESSAGE_PUBLISHED_TIME, message.getPublishTime());
        msgCtx.setProperty(GooglePubSubConstants.MESSAGE_ORDERING_KEY, message.getOrderingKey());
        message.getAttributesMap().forEach((key, value) -> msgCtx.setProperty(key, value));
        msgCtx.setProperty(SynapseConstants.IS_INBOUND, true);
        return msgCtx;
    }

    private MessageContext createMessageContext() {
        MessageContext msgCtx = this.synapseEnvironment.createMessageContext();
        org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(String.valueOf(UUID.randomUUID()));
        return msgCtx;
    }

    private void loadConfiguration(Properties properties) {
        projectId = properties.getProperty(GooglePubSubConstants.PROJECT_ID);
        subscriptionId = properties.getProperty(GooglePubSubConstants.SUBSCRIPTION_ID);
        contentType = properties.getProperty(GooglePubSubConstants.CONTENT_TYPE,
                GooglePubSubConstants.DEFAULT_CONTENT_TYPE);
        keyFilePath = properties.getProperty(GooglePubSubConstants.KEY_FILE_PATH);
        maxOutstandingMessageCount= Long.parseLong(
                properties.getProperty(GooglePubSubConstants.MAX_MESSAGE_COUNT,GooglePubSubConstants.DEFAULT_MAX_MESSAGE_COUNT));
        maxOutstandingMessageSize= Long.parseLong(
                properties.getProperty(GooglePubSubConstants.MAX_MESSAGE_SIZE,GooglePubSubConstants.DEFAULT_MAX_MESSAGE_SIZE_IN_MB));
        executorThreadsPerConsumers = Integer.parseInt(
                properties.getProperty(GooglePubSubConstants.EXECUTOR_THREADS_PER_CONSUMERS,
                        GooglePubSubConstants.DEFAULT_EXECUTOR_THREADS_PER_CONSUMERS));
        concurrentConsumers = Integer.parseInt(properties.getProperty(GooglePubSubConstants.CONCURRENT_CONSUMERS,
                GooglePubSubConstants.DEFAULT_CONCURRENT_CONSUMERS));
        endpoint = properties.getProperty(GooglePubSubConstants.ENDPOINT, GooglePubSubConstants.DEFAULT_ENDPOINT);
        createSubscriptionOnConnect = Boolean.parseBoolean(
                properties.getProperty(GooglePubSubConstants.CREATE_SUBSCRIPTION_ON_CONNECT, "false"));
        updateSubscriptionIfExists = Boolean.parseBoolean(
                properties.getProperty(GooglePubSubConstants.UPDATE_SUBSCRIPTION_IF_EXISTS, "false"));


        String topicId = properties.getProperty(GooglePubSubConstants.TOPIC_ID);
        String filter = properties.getProperty(GooglePubSubConstants.MESSAGE_FILTER, "");
        boolean enableMessageOrdering = Boolean.parseBoolean(
                properties.getProperty(GooglePubSubConstants.ENABLE_MESSAGE_ORDERING,"false"));
        int ackDeadlineSeconds = Integer.parseInt(properties.getProperty(GooglePubSubConstants.ACK_DEADLINE_SECONDS,
                GooglePubSubConstants.DEFAULT_ACK_DEADLINE_SECONDS));
        String deadLetterTopic = properties.getProperty(GooglePubSubConstants.DEAD_LETTER_TOPIC);
        int maxDeliveryAttempts = Integer.parseInt(properties.getProperty(GooglePubSubConstants.MAX_DELIVERY_ATTEMPTS,GooglePubSubConstants.DEFAULT_MAX_DELIVERY_ATTEMPTS));
        long messageRetentionDuration = Long.parseLong(
                properties.getProperty(GooglePubSubConstants.MESSAGE_RETENTION_DURATION, GooglePubSubConstants.DEFAULT_MESSAGE_RETENTION_DURATION));
        boolean exactlyOnceDelivery = Boolean.parseBoolean(
                properties.getProperty(GooglePubSubConstants.EXACTLY_ONCE_DELIVERY, "false"));
        long minBackOffDuration = Long.parseLong(properties.getProperty(GooglePubSubConstants.MIN_BACKOFF_DURATION,GooglePubSubConstants.DEFAULT_MIN_BACKOFF_DURATION));
        long maxBackOffDuration = Long.parseLong(properties.getProperty(GooglePubSubConstants.MAX_BACKOFF_DURATION,GooglePubSubConstants.DEFAULT_MAX_BACKOFF_DURATION));
        boolean retainAckedMessages = Boolean.parseBoolean(properties.getProperty(GooglePubSubConstants.RETAIN_ACKED_MESSAGES,"false"));
        String labels = properties.getProperty(GooglePubSubConstants.LABELS);
        DeadLetterPolicy deadLetterPolicy=null;
        if(deadLetterTopic != null){
            deadLetterPolicy = DeadLetterPolicy.newBuilder()
                    .setDeadLetterTopic(String.valueOf(ProjectTopicName.of(projectId, deadLetterTopic)))
                    .setMaxDeliveryAttempts(maxDeliveryAttempts).build();
        }

        RetryPolicy retryPolicy = RetryPolicy.newBuilder().setMinimumBackoff(
                        com.google.protobuf.Duration.newBuilder().setSeconds(minBackOffDuration).build())
                .setMaximumBackoff(
                        com.google.protobuf.Duration.newBuilder().setSeconds(maxBackOffDuration).build())
                .build();
        try {
            if(keyFilePath== null || keyFilePath.isEmpty()){
                credentials = GoogleCredentials.getApplicationDefault();

            }else{

                credentials = GoogleCredentials.fromStream(
                        new FileInputStream(keyFilePath)
                );
            }
            subscriptionManager = new GooglePubSubSubscriptionManager(projectId, topicId, subscriptionId, filter,
                    ackDeadlineSeconds, deadLetterPolicy, updateSubscriptionIfExists, exactlyOnceDelivery,
                    messageRetentionDuration, retryPolicy, labels,credentials,enableMessageOrdering,retainAckedMessages);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean injectMessage(InputStream in, String contentType, MessageContext msgCtx) {
        boolean isConsumed = true;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Processed Custom inbound EP Message of Content-type : " + contentType + " for " + name);
            }

            org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
            Object builder;
            if (StringUtils.isEmpty(contentType)) {
                log.warn("Unable to determine content type for message, setting to application/json for " + name);
            }
            int index = contentType.indexOf(';');
            String type = index > 0 ? contentType.substring(0, index) : contentType;
            builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
            if (builder == null) {
                if (log.isDebugEnabled()) {
                    log.debug("No message builder found for type '" + type + "'. Falling back to SOAP. for" + name);
                }
                builder = new SOAPBuilder();
            }

            OMElement documentElement1 = ((Builder) builder).processDocument(in, contentType, axis2MsgCtx);
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement1));
            if (this.injectingSeq == null || "".equals(this.injectingSeq)) {
                log.error("Sequence name not specified. Sequence : " + this.injectingSeq + " for " + name);
                isConsumed = false;
            }
            SequenceMediator seq = (SequenceMediator) this.synapseEnvironment.getSynapseConfiguration()
                    .getSequence(this.injectingSeq);
            if (seq == null) {
                throw new SynapseException(
                        "Sequence with name : " + this.injectingSeq + " is not found to mediate the message.");
            }
            seq.setErrorHandler(this.onErrorSeq);
            if (log.isDebugEnabled()) {
                log.debug("injecting message to sequence : " + this.injectingSeq + " of " + name);
            }
            if (!this.synapseEnvironment.injectInbound(msgCtx, seq, this.sequential)) {
                isConsumed = false;
            }if (isRollback(msgCtx)) {
                isConsumed = false;
            }
        } catch (Exception e) {
            log.error(
                    "Error while processing the Google Pub/Sub inbound endpoint Message and the message should be in the format of " + contentType,
                    e);
            isConsumed = false;
        }
        return isConsumed;
    }

    public void receiveMessages(String projectId, String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            log.debug("Received message ID: " + message.getMessageId());
            log.debug("Data: " + message.getData().toStringUtf8());
            log.debug("Attributes: " + message.getAttributesMap());
            log.debug("Ordering Key: "+ message.getOrderingKey());
            log.debug("Publish Time: "+ message.getPublishTime());
            log.debug("Thread ID: " + Thread.currentThread().getId() +
                    " | Name: " + Thread.currentThread().getName() +
                    " | Message ID: " + message.getMessageId() +
                    " | Ordering Key: " + message.getOrderingKey());
            MessageContext msgCtx = populateMessageContext(message);
            isConsumed = injectMessage(message.getData().toStringUtf8(), contentType, msgCtx);
            if (isConsumed) {
                consumer.ack();
            } else {
                log.info("Processing failed. Hence sending an nack for message: " + message.getMessageId() + "  Re-delivery Attempt: " + message.getAttributesMap().get("googclient_deliveryattempt"));
                consumer.nack();
            }
        };
        FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder().setMaxOutstandingElementCount(maxOutstandingMessageCount)
                .setMaxOutstandingRequestBytes(maxOutstandingMessageSize * 1024L * 1024L).build();
        ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(executorThreadsPerConsumers)
                .setThreadFactory(r -> new Thread(r, GooglePubSubConstants.GOOGLE_PUBSUB_CONSUMER_THREAD_NAME + threadId.getAndIncrement()))
                .build();

        subscriber = Subscriber.newBuilder(subscriptionName, receiver).setEndpoint(endpoint)
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .setFlowControlSettings(flowControlSettings).setExecutorProvider(executorProvider)
                .setParallelPullCount(concurrentConsumers).build();

        subscriber.addListener(new Subscriber.Listener() {
            public void failed(Subscriber.State from, Throwable failure) {
                log.info("Unrecoverable subscriber failure:" + failure.getStackTrace());
            }
        }, MoreExecutors.directExecutor());
        subscriber.startAsync().awaitRunning();
        log.info("Google Pub/Sub Subscriber Started.");
        log.info("Listening for messages on Subscription: " + subscriptionName + " Subscriber state: " + subscriber.state());
    }

    private boolean isRollback(MessageContext msgCtx) {
        // check rollback property from synapse context
        Object rollbackProp = msgCtx.getProperty(GooglePubSubConstants.SET_ROLLBACK_ONLY);
        if (rollbackProp != null) {
            return (rollbackProp instanceof Boolean && ((Boolean) rollbackProp))
                    || (rollbackProp instanceof String && Boolean.valueOf((String) rollbackProp));
        }//TO-DO: operational context
        return false;
    }


}
