/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.opensearch.plugin.kinesis.SequenceNumber.NON_EXISTING_SEQUENCE_NUMBER;
import static software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create;

/**
 * Kinesis consumer using enhanced fan-out (SubscribeToShard) to read messages from a Kinesis shard.
 * <p>
 * Enhanced fan-out delivers records over a long-lived HTTP/2 push subscription with dedicated
 * throughput per registered consumer, instead of sharing the 2 MB/s / 5 TPS GetRecords limits with
 * other consumers of the stream. Since the core ingestion contract ({@link
 * org.opensearch.index.IngestionShardConsumer#readNext}) is a synchronous pull, this class bridges
 * the push subscription into a pull model: pushed events are buffered in a demand-controlled queue
 * (at most one event is requested at a time) and drained by {@code readNext}. Subscriptions expire
 * after five minutes and are transparently renewed from the last fetched sequence number.
 * <p>
 * Positioned reads ({@code readNext(pointer, ...)}) may target any position (poller initialization,
 * error retry, offset reset), so they always discard the current subscription and any buffered
 * records, and re-subscribe at the requested sequence number. Pointer resolution
 * (earliest/latest/timestamp) reuses the synchronous GetRecords implementation of the parent class,
 * as those are rare one-shot operations.
 */
@SuppressWarnings("removal")
public class KinesisEfoShardConsumer extends KinesisShardConsumer {
    private static final Logger logger = LogManager.getLogger(KinesisEfoShardConsumer.class);

    /**
     * Number of consecutive subscription attempts that terminated with an error before any event
     * was received, after which the error is surfaced to the poller (which will pause ingestion).
     */
    static final int MAX_CONSECUTIVE_SUBSCRIPTION_FAILURES = 3;

    // SubscribeToShard natively supports TRIM_HORIZON / LATEST starting positions, so earliest/latest
    // pointers are not resolved to a concrete sequence number (which the synchronous GetRecords probe
    // cannot do for an empty shard - it returns NON_EXISTING_SEQUENCE_NUMBER, an invalid sequence
    // number for the API). Instead earliestPointer()/latestPointer() return these sentinels and
    // readNext maps them to the corresponding native starting position. The sentinel strings are
    // non-numeric, so they can never collide with a real Kinesis sequence number.
    static final SequenceNumber TRIM_HORIZON_POINTER = new SequenceNumber("trim-horizon");
    static final SequenceNumber LATEST_POINTER = new SequenceNumber("latest");

    private final KinesisAsyncClient kinesisAsyncClient;
    // resolved enhanced fan-out consumer ARN to subscribe with (from config, or auto-registered by the factory)
    private final String consumerArn;

    // records fetched from the stream but not yet returned to the poller (readNext maxMessages overflow)
    private final Deque<Record> pendingRecords = new ArrayDeque<>();
    private EfoSubscription activeSubscription;
    // position used to open the current subscription; resubscribe target until the first record arrives
    private StartingPosition currentStartingPosition;
    // sequence number of the last record taken off the event stream (returned or pending)
    private String lastFetchedSequenceNumber;
    private int consecutiveSubscriptionFailures;
    private Throwable lastSubscriptionFailure;

    /**
     * Constructor
     * @param clientId the client id
     * @param config   the kinesis source config
     * @param shardId the shard id
     * @param consumerArn the resolved enhanced fan-out consumer ARN to subscribe with
     */
    public KinesisEfoShardConsumer(String clientId, KinesisSourceConfig config, int shardId, String consumerArn) {
        this(clientId, config, shardId, consumerArn, createClient(clientId, config), createAsyncClient(clientId, config));
    }

    /**
     * Constructor, visible for testing
     * @param clientId the client id
     * @param config the Kinesis source config
     * @param shardId the shard id
     * @param consumerArn the resolved enhanced fan-out consumer ARN to subscribe with
     * @param kinesisClient the synchronous kinesis client, used for shard resolution and pointer lookups
     * @param kinesisAsyncClient the asynchronous kinesis client, used for SubscribeToShard
     */
    protected KinesisEfoShardConsumer(
        String clientId,
        KinesisSourceConfig config,
        int shardId,
        String consumerArn,
        KinesisClient kinesisClient,
        KinesisAsyncClient kinesisAsyncClient
    ) {
        super(clientId, config, shardId, kinesisClient);
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.consumerArn = consumerArn;
        logger.info("kinesis EFO consumer created for stream {} shard {} consumer arn {}", config.getStream(), shardId, consumerArn);
    }

    /**
     * Create the asynchronous Kinesis client used for SubscribeToShard. Visible for testing.
     * @param clientId the client id
     * @param config the Kinesis source config
     * @return the asynchronous Kinesis client
     */
    protected static KinesisAsyncClient createAsyncClient(String clientId, KinesisSourceConfig config) {
        setDefaultAwsProfilePath();

        KinesisAsyncClientBuilder builder = KinesisAsyncClient.builder()
            .region(Region.of(config.getRegion()))
            // TODO: better security config
            .credentialsProvider(StaticCredentialsProvider.create(create(config.getAccessKey(), config.getSecretKey())))
            // SubscribeToShard requires HTTP/2
            .httpClientBuilder(NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP2));

        if (config.getEndpointOverride() != null && !config.getEndpointOverride().isEmpty()) {
            try {
                builder = builder.endpointOverride(new URI(config.getEndpointOverride()));
            } catch (URISyntaxException e) {
                throw new RuntimeException("Invalid endpoint override: " + config.getEndpointOverride(), e);
            }
        }

        final KinesisAsyncClientBuilder asyncBuilder = builder;
        // building the client (and the Netty HTTP/2 client) resolves credentials/region and may touch
        // restricted resources; run privileged so it executes under the plugin's security policy
        return AccessController.doPrivileged((PrivilegedAction<KinesisAsyncClient>) asyncBuilder::build);
    }

    @Override
    public synchronized List<ReadResult<SequenceNumber, KinesisMessage>> readNext(
        SequenceNumber sequenceNumber,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        // A positioned read may target any position (initialization, retry of a failed batch,
        // offset reset), so buffered records and the current subscription cannot be reused.
        StartingPosition startingPosition = toStartingPosition(sequenceNumber, includeStart);
        resetSubscription(startingPosition);
        return poll(maxMessages, timeoutMillis);
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return TRIM_HORIZON_POINTER;
    }

    @Override
    public IngestionShardPointer latestPointer() {
        return LATEST_POINTER;
    }

    /**
     * Translate an ingestion pointer into a SubscribeToShard {@link StartingPosition}. The earliest and
     * latest sentinels (see {@link #TRIM_HORIZON_POINTER} / {@link #LATEST_POINTER}) map to the native
     * TRIM_HORIZON / LATEST positions; the "no records yet" sentinel
     * ({@link SequenceNumber#NON_EXISTING_SEQUENCE_NUMBER}, which can still arrive from a timestamp-based
     * reset on an empty shard) is treated as the start of the shard; any other value is a concrete
     * sequence number consumed at (includeStart) or after the given position.
     */
    private StartingPosition toStartingPosition(SequenceNumber sequenceNumber, boolean includeStart) {
        String sequence = sequenceNumber.getSequenceNumber();
        if (TRIM_HORIZON_POINTER.getSequenceNumber().equals(sequence)
            || NON_EXISTING_SEQUENCE_NUMBER.getSequenceNumber().equals(sequence)) {
            return StartingPosition.builder().type(ShardIteratorType.TRIM_HORIZON).build();
        }
        if (LATEST_POINTER.getSequenceNumber().equals(sequence)) {
            return StartingPosition.builder().type(ShardIteratorType.LATEST).build();
        }
        ShardIteratorType iteratorType = includeStart ? ShardIteratorType.AT_SEQUENCE_NUMBER : ShardIteratorType.AFTER_SEQUENCE_NUMBER;
        return StartingPosition.builder().type(iteratorType).sequenceNumber(sequence).build();
    }

    @Override
    public synchronized List<ReadResult<SequenceNumber, KinesisMessage>> readNext(long maxMessages, int timeoutMillis)
        throws TimeoutException {
        if (activeSubscription == null && currentStartingPosition == null) {
            throw new IllegalStateException("No active subscription");
        }
        return poll(maxMessages, timeoutMillis);
    }

    private void resetSubscription(StartingPosition startingPosition) {
        if (activeSubscription != null) {
            activeSubscription.cancel();
            activeSubscription = null;
        }
        pendingRecords.clear();
        lastFetchedSequenceNumber = null;
        consecutiveSubscriptionFailures = 0;
        openSubscription(startingPosition);
    }

    private void openSubscription(StartingPosition startingPosition) {
        EfoSubscription subscription = new EfoSubscription();
        SubscribeToShardRequest request = SubscribeToShardRequest.builder()
            .consumerARN(consumerArn)
            .shardId(kinesisShardId)
            .startingPosition(startingPosition)
            .build();
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
            .onError(subscription::fail)
            .onComplete(subscription::complete)
            .onEventStream(publisher -> publisher.subscribe(subscription))
            .build();
        // initiating the async request drives the Netty HTTP/2 client, which may create threads and
        // load classes; run privileged so it executes under the plugin's security policy
        CompletableFuture<Void> subscriptionFuture = AccessController.doPrivileged(
            (PrivilegedAction<CompletableFuture<Void>>) () -> kinesisAsyncClient.subscribeToShard(request, responseHandler)
        );
        subscriptionFuture.whenComplete((ignored, e) -> {
            if (e != null) {
                subscription.fail(e);
            } else {
                subscription.complete();
            }
        });
        currentStartingPosition = startingPosition;
        activeSubscription = subscription;
        logger.debug("opened EFO subscription for shard {} at {}", kinesisShardId, startingPosition);
    }

    private List<ReadResult<SequenceNumber, KinesisMessage>> poll(long maxMessages, int timeoutMillis) {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        List<ReadResult<SequenceNumber, KinesisMessage>> results = new ArrayList<>();

        while (results.size() < maxMessages && pendingRecords.isEmpty() == false) {
            results.add(toReadResult(pendingRecords.poll()));
        }

        while (results.size() < maxMessages) {
            ensureActiveSubscription();

            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            // once some records are available, only drain what has already arrived
            long waitMillis = results.isEmpty() ? Math.max(remainingMillis, 0) : 0;

            SubscribeToShardEvent event;
            try {
                event = activeSubscription.pollEvent(waitMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (event == null) {
                if (activeSubscription.isTerminated()) {
                    handleSubscriptionEnd();
                    if (remainingMillis > 0) {
                        continue;
                    }
                }
                break;
            }

            consecutiveSubscriptionFailures = 0;
            for (Record record : event.records()) {
                lastFetchedSequenceNumber = record.sequenceNumber();
                if (results.size() < maxMessages) {
                    results.add(toReadResult(record));
                } else {
                    pendingRecords.add(record);
                }
            }

            if (TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()) <= 0) {
                break;
            }
        }

        return results;
    }

    /**
     * Renew the subscription if the previous one terminated. Subscriptions end normally every five
     * minutes; the renewed subscription resumes after the last fetched record so that buffered
     * pending records are not fetched twice.
     */
    private void ensureActiveSubscription() {
        if (activeSubscription != null && activeSubscription.isTerminated() == false) {
            return;
        }

        if (activeSubscription != null) {
            handleSubscriptionEnd();
        }

        StartingPosition resumePosition = lastFetchedSequenceNumber != null
            ? StartingPosition.builder().type(ShardIteratorType.AFTER_SEQUENCE_NUMBER).sequenceNumber(lastFetchedSequenceNumber).build()
            : currentStartingPosition;
        openSubscription(resumePosition);
    }

    private void handleSubscriptionEnd() {
        if (activeSubscription == null) {
            return;
        }

        Throwable failure = activeSubscription.failure();
        boolean receivedAnyEvent = activeSubscription.receivedAnyEvent();
        activeSubscription = null;

        if (failure != null) {
            lastSubscriptionFailure = failure;
        }

        if (failure == null && receivedAnyEvent) {
            // normal completion: the 5-minute subscription window expired
            logger.debug("EFO subscription for shard {} completed, will renew", kinesisShardId);
            return;
        }

        // a subscription that terminated without delivering a single event (error, or an abnormal
        // instant completion) is counted so that repeated failures surface to the poller instead of
        // silently resubscribing forever
        if (receivedAnyEvent == false) {
            consecutiveSubscriptionFailures++;
        }
        if (consecutiveSubscriptionFailures >= MAX_CONSECUTIVE_SUBSCRIPTION_FAILURES) {
            throw new RuntimeException(
                "EFO subscription for shard "
                    + kinesisShardId
                    + " terminated "
                    + consecutiveSubscriptionFailures
                    + " consecutive times without delivering events",
                lastSubscriptionFailure
            );
        }
        logger.warn(() -> new ParameterizedMessage("EFO subscription for shard {} terminated, will resubscribe", kinesisShardId), failure);
    }

    private ReadResult<SequenceNumber, KinesisMessage> toReadResult(Record record) {
        SequenceNumber sequenceNumber = new SequenceNumber(record.sequenceNumber());
        Long timestamp = record.approximateArrivalTimestamp() != null ? record.approximateArrivalTimestamp().toEpochMilli() : null;
        KinesisMessage message = new KinesisMessage(record.data().asByteArray(), timestamp);
        return new ReadResult<>(sequenceNumber, message);
    }

    @Override
    public synchronized void close() throws IOException {
        if (activeSubscription != null) {
            activeSubscription.cancel();
            activeSubscription = null;
        }
        if (kinesisAsyncClient != null) {
            kinesisAsyncClient.close();
        }
        super.close();
    }

    /**
     * Bridges the reactive SubscribeToShard event stream into a blocking queue consumed by
     * {@code readNext}. Demand is controlled by requesting one event at a time: the next event is
     * requested only after the previous one has been taken off the queue, so backpressure from the
     * poller propagates to the subscription.
     */
    static class EfoSubscription implements Subscriber<SubscribeToShardEventStream> {
        private final BlockingQueue<SubscribeToShardEvent> eventQueue = new LinkedBlockingQueue<>();
        private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        private volatile Throwable failure;
        private volatile boolean done;
        private volatile boolean receivedAnyEvent;

        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(subscription);
            subscription.request(1);
        }

        @Override
        public void onNext(SubscribeToShardEventStream event) {
            if (event instanceof SubscribeToShardEvent) {
                SubscribeToShardEvent shardEvent = (SubscribeToShardEvent) event;
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "EFO event records={} millisBehindLatest={}",
                        shardEvent.records().size(),
                        shardEvent.millisBehindLatest()
                    );
                }
                receivedAnyEvent = true;
                eventQueue.add(shardEvent);
            } else {
                // unknown event type: keep the stream flowing
                requestNext();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            fail(throwable);
        }

        @Override
        public void onComplete() {
            done = true;
        }

        void fail(Throwable throwable) {
            if (failure == null) {
                failure = throwable;
            }
            done = true;
        }

        void complete() {
            done = true;
        }

        /**
         * Take the next event off the queue, waiting up to the given timeout. Returns null if no
         * event is available in time or the subscription has terminated.
         */
        SubscribeToShardEvent pollEvent(long timeoutMillis) throws InterruptedException {
            SubscribeToShardEvent event = eventQueue.poll();
            if (event == null && done == false && timeoutMillis > 0) {
                event = eventQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            if (event != null) {
                requestNext();
            }
            return event;
        }

        boolean isTerminated() {
            return done && eventQueue.isEmpty();
        }

        Throwable failure() {
            return failure;
        }

        boolean receivedAnyEvent() {
            return receivedAnyEvent;
        }

        void cancel() {
            done = true;
            Subscription subscription = subscriptionRef.get();
            if (subscription != null) {
                subscription.cancel();
            }
        }

        private void requestNext() {
            Subscription subscription = subscriptionRef.get();
            if (subscription != null && done == false) {
                subscription.request(1);
            }
        }
    }
}
