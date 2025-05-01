/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.Term;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 *  A class to process messages from the ingestion stream. It extracts the payload from the message and creates an
 *  engine operation.
 */
public class MessageProcessorRunnable implements Runnable, Closeable {
    private static final Logger logger = LogManager.getLogger(MessageProcessorRunnable.class);
    private static final String ID = "_id";
    private static final String OP_TYPE = "_op_type";
    private static final String SOURCE = "_source";
    private static final int WAIT_BEFORE_RETRY_DURATION_MS = 5000;

    private final BlockingQueue<ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message>> blockingQueue;
    private final MessageProcessor messageProcessor;
    private final MessageProcessorMetrics messageProcessorMetrics = MessageProcessorMetrics.create();

    // currentShardPointer tracks the most recent pointer that is being processed
    @Nullable
    private volatile IngestionShardPointer currentShardPointer;
    private volatile boolean closed = false;
    private volatile IngestionErrorStrategy errorStrategy;

    /**
     * Constructor.
     *
     * @param blockingQueue the blocking queue to poll messages from
     * @param engine the ingestion engine
     */
    public MessageProcessorRunnable(
        BlockingQueue<ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message>> blockingQueue,
        IngestionEngine engine,
        IngestionErrorStrategy errorStrategy
    ) {
        this(blockingQueue, new MessageProcessor(engine), errorStrategy);
    }

    /**
     * Constructor visible for testing.
     * @param blockingQueue the blocking queue to poll messages from
     * @param messageProcessor the message processor
     */
    MessageProcessorRunnable(
        BlockingQueue<ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message>> blockingQueue,
        MessageProcessor messageProcessor,
        IngestionErrorStrategy errorStrategy
    ) {
        this.blockingQueue = Objects.requireNonNull(blockingQueue);
        this.messageProcessor = messageProcessor;
        this.errorStrategy = errorStrategy;
    }

    static class MessageProcessor {
        private final IngestionEngine engine;
        private final String index;

        MessageProcessor(IngestionEngine engine) {
            this(engine, engine.config().getIndexSettings().getIndex().getName());
        }

        /**
         *  visible for testing
         * @param engine the ingestion engine
         * @param index the index name
         */
        MessageProcessor(IngestionEngine engine, String index) {
            this.engine = engine;
            this.index = index;
        }

        /**
         * Visible for testing. Process the message and create an engine operation.
         *
         * Process the message and create an engine operation. It also records the offset in the document as (1) a point
         * field used for range search, (2) a stored field for retrieval.
         *
         * @param shardUpdateMessage contains the message to process
         * @param messageProcessorMetrics message processor metrics
         */
        protected void process(ShardUpdateMessage shardUpdateMessage, MessageProcessorMetrics messageProcessorMetrics) {
            try {
                Engine.Operation operation = getOperation(shardUpdateMessage, messageProcessorMetrics);
                switch (operation.operationType()) {
                    case INDEX:
                        engine.indexInternal((Engine.Index) operation);
                        break;
                    case DELETE:
                        engine.deleteInternal((Engine.Delete) operation);
                        break;
                    case NO_OP:
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid operation: " + operation);
                }
            } catch (IOException e) {
                logger.error(
                    "Failed to process operation from message {} at pointer {}: {}",
                    shardUpdateMessage.originalMessage(),
                    shardUpdateMessage.pointer().asString(),
                    e
                );
                throw new RuntimeException(e);
            }
        }

        /**
         * Visible for testing. Get the engine operation from the message.
         * @param shardUpdateMessage an update message containing payload and pointer for the update
         * @param messageProcessorMetrics message processor metrics
         * @return the engine operation
         */
        protected Engine.Operation getOperation(ShardUpdateMessage shardUpdateMessage, MessageProcessorMetrics messageProcessorMetrics)
            throws IOException {
            Map<String, Object> payloadMap = shardUpdateMessage.parsedPayloadMap();
            IngestionShardPointer pointer = shardUpdateMessage.pointer();

            if (payloadMap.containsKey(OP_TYPE) && !(payloadMap.get(OP_TYPE) instanceof String)) {
                messageProcessorMetrics.invalidMessageCounter.inc();
                logger.error("_op_type field is of type {} but not string, skipping the message", payloadMap.get(OP_TYPE).getClass());
                return null;
            }

            if (payloadMap.containsKey(ID) == false) {
                messageProcessorMetrics.invalidMessageCounter.inc();
                logger.error("ID field is missing, skipping the message");
                return null;
            }

            String id = (String) payloadMap.get(ID);
            String opTypeString = (String) payloadMap.getOrDefault(OP_TYPE, "index");
            DocWriteRequest.OpType opType = DocWriteRequest.OpType.fromString(opTypeString);

            // Check message for document version. Pull-based ingestion only supports external versioning.
            // By default, writes succeed regardless of document version.
            long documentVersion = Versions.MATCH_ANY;
            VersionType documentVersionType = VersionType.INTERNAL;
            if (payloadMap.containsKey(VersionFieldMapper.NAME)) {
                documentVersion = Long.parseLong((String) payloadMap.get(VersionFieldMapper.NAME));
                documentVersionType = VersionType.EXTERNAL;
            }

            Engine.Operation operation;
            switch (opType) {
                case INDEX:
                    if (!payloadMap.containsKey(SOURCE)) {
                        messageProcessorMetrics.invalidMessageCounter.inc();
                        logger.error("missing _source field, skipping the message");
                        return null;
                    }
                    if (!(payloadMap.get(SOURCE) instanceof Map)) {
                        messageProcessorMetrics.invalidMessageCounter.inc();
                        logger.error("_source field does not contain a map, skipping the message");
                        return null;
                    }
                    BytesReference source = convertToBytes(payloadMap.get(SOURCE));

                    SourceToParse sourceToParse = new SourceToParse(index, id, source, MediaTypeRegistry.xContentType(source), null);
                    ParsedDocument doc = engine.getDocumentMapperForType().getDocumentMapper().parse(sourceToParse);
                    ParseContext.Document document = doc.rootDoc();
                    // set the offset as the offset field
                    document.add(pointer.asPointField(IngestionShardPointer.OFFSET_FIELD));
                    // store the offset as string in stored field
                    document.add(new StoredField(IngestionShardPointer.OFFSET_FIELD, pointer.asString()));

                    operation = new Engine.Index(
                        new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
                        doc,
                        0,
                        1,
                        documentVersion,
                        documentVersionType,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        shardUpdateMessage.autoGeneratedIdTimestamp(),
                        false,
                        UNASSIGNED_SEQ_NO,
                        0
                    );
                    break;
                case DELETE:
                    if (shardUpdateMessage.autoGeneratedIdTimestamp() != UNSET_AUTO_GENERATED_TIMESTAMP) {
                        logger.info("Delete operation without ID received, and will be dropped.");
                        messageProcessorMetrics.invalidMessageCounter.inc();
                        operation = new Engine.NoOp(
                            0,
                            1,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            "Delete operation is missing ID. Skipping message."
                        );
                    } else {
                        operation = new Engine.Delete(
                            id,
                            new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
                            0,
                            1,
                            documentVersion,
                            documentVersionType,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            UNASSIGNED_SEQ_NO,
                            0
                        );
                    }
                    break;
                default:
                    messageProcessorMetrics.invalidMessageCounter.inc();
                    logger.error("Unsupported operation type {}", opType);
                    return null;
            }

            return operation;
        }
    }

    private static BytesReference convertToBytes(Object object) throws IOException {
        assert object instanceof Map;
        return BytesReference.bytes(XContentFactory.jsonBuilder().map((Map) object));
    }

    BlockingQueue<ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message>> getBlockingQueue() {
        return blockingQueue;
    }

    /**
     * Polls messages from the blocking queue and processes messages. If message processing fails, the failed message
     * is retried indefinitely after a retry wait time, unless a DROP error policy is used to skip the failed message.
     */
    @Override
    public void run() {
        ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message> shardUpdateMessage = null;

        while (Thread.currentThread().isInterrupted() == false && closed == false) {
            try {
                if (shardUpdateMessage == null) {
                    shardUpdateMessage = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                messageProcessorMetrics.processorThreadInterruptCounter.inc();
                logger.debug("MessageProcessorRunnable poll interruptedException", e);
                Thread.currentThread().interrupt(); // Restore interrupt status
            }
            if (shardUpdateMessage != null) {
                try {
                    messageProcessorMetrics.processedCounter.inc();
                    currentShardPointer = shardUpdateMessage.pointer();
                    messageProcessor.process(shardUpdateMessage, messageProcessorMetrics);
                    shardUpdateMessage = null;
                } catch (VersionConflictEngineException e) {
                    // Messages with version conflicts will be dropped. This should not have any impact to data
                    // correctness as pull-based ingestion does not support partial updates.
                    messageProcessorMetrics.versionConflictCounter.inc();
                    logger.debug("Dropping message due to version conflict. ShardPointer: " + shardUpdateMessage.pointer().asString(), e);
                    shardUpdateMessage = null;
                } catch (Exception e) {
                    messageProcessorMetrics.failedMessageCounter.inc();
                    errorStrategy.handleError(e, IngestionErrorStrategy.ErrorStage.PROCESSING);
                    if (errorStrategy.shouldIgnoreError(e, IngestionErrorStrategy.ErrorStage.PROCESSING)) {
                        shardUpdateMessage = null;
                        messageProcessorMetrics.failedMessageDroppedCounter.inc();
                    } else {
                        waitBeforeRetry();
                    }
                }
            }
        }
    }

    private void waitBeforeRetry() {
        try {
            Thread.sleep(WAIT_BEFORE_RETRY_DURATION_MS);
        } catch (InterruptedException e) {
            logger.debug("MessageProcessor thread interrupted while waiting for retry", e);
            Thread.currentThread().interrupt(); // Restore interrupt status
        }
    }

    public MessageProcessorMetrics getMessageProcessorMetrics() {
        return messageProcessorMetrics;
    }

    public IngestionErrorStrategy getErrorStrategy() {
        return this.errorStrategy;
    }

    public void setErrorStrategy(IngestionErrorStrategy errorStrategy) {
        this.errorStrategy = errorStrategy;
    }

    @Nullable
    public IngestionShardPointer getCurrentShardPointer() {
        return currentShardPointer;
    }

    /**
     * Closes and stops the message processor.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * Tracks MessageProcessor metrics.
     */
    public record MessageProcessorMetrics(CounterMetric processedCounter, CounterMetric invalidMessageCounter,
        CounterMetric versionConflictCounter, CounterMetric failedMessageCounter, CounterMetric failedMessageDroppedCounter,
        CounterMetric processorThreadInterruptCounter) {
        public static MessageProcessorMetrics create() {
            return new MessageProcessorMetrics(
                new CounterMetric(),
                new CounterMetric(),
                new CounterMetric(),
                new CounterMetric(),
                new CounterMetric(),
                new CounterMetric()
            );
        }

        public MessageProcessorMetrics combine(MessageProcessorMetrics other) {
            MessageProcessorMetrics combinedMetrics = create();
            combinedMetrics.processedCounter.inc(this.processedCounter.count() + other.processedCounter.count());
            combinedMetrics.invalidMessageCounter.inc(this.invalidMessageCounter.count() + other.invalidMessageCounter.count());
            combinedMetrics.versionConflictCounter.inc(this.versionConflictCounter.count() + other.versionConflictCounter.count());
            combinedMetrics.failedMessageCounter.inc(this.failedMessageCounter.count() + other.failedMessageCounter.count());
            combinedMetrics.failedMessageDroppedCounter.inc(
                this.failedMessageDroppedCounter.count() + other.failedMessageDroppedCounter.count()
            );
            combinedMetrics.processorThreadInterruptCounter.inc(
                this.processorThreadInterruptCounter.count() + other.processorThreadInterruptCounter.count()
            );

            return combinedMetrics;
        }
    }
}
