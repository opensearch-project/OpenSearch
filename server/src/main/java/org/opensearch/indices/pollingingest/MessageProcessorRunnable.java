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
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 *  A class to process messages from the ingestion stream. It extracts the payload from the message and creates an
 *  engine operation.
 */
public class MessageProcessorRunnable implements Runnable {
    private static final Logger logger = LogManager.getLogger(MessageProcessorRunnable.class);

    private final BlockingQueue<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> blockingQueue;
    private final MessageProcessor messageProcessor;

    private static final String ID = "_id";
    private static final String OP_TYPE = "_op_type";
    private static final String SOURCE = "_source";

    /**
     * Constructor.
     *
     * @param blockingQueue the blocking queue to poll messages from
     * @param engine the ingestion engine
     */
    public MessageProcessorRunnable(
        BlockingQueue<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> blockingQueue,
        IngestionEngine engine
    ) {
        this(blockingQueue, new MessageProcessor(engine));
    }

    /**
     * Constructor visible for testing.
     * @param blockingQueue the blocking queue to poll messages from
     * @param messageProcessor the message processor
     */
    MessageProcessorRunnable(
        BlockingQueue<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> blockingQueue,
        MessageProcessor messageProcessor
    ) {
        this.blockingQueue = Objects.requireNonNull(blockingQueue);
        this.messageProcessor = messageProcessor;
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
         * @param message the message to process
         * @param pointer the pointer to the message
         */
        protected void process(Message message, IngestionShardPointer pointer) {
            byte[] payload = (byte[]) message.getPayload();

            try {
                Engine.Operation operation = getOperation(payload, pointer);
                switch (operation.operationType()) {
                    case INDEX:
                        engine.index((Engine.Index) operation);
                        break;
                    case DELETE:
                        engine.delete((Engine.Delete) operation);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid operation: " + operation);
                }
            } catch (IOException e) {
                logger.error("Failed to process operation from message {} at pointer {}: {}", message, pointer, e);
                throw new RuntimeException(e);
            }
        }

        /**
         * Visible for testing. Get the engine operation from the message.
         * @param payload the payload of the message
         * @param pointer the pointer to the message
         * @return the engine operation
         */
        protected Engine.Operation getOperation(byte[] payload, IngestionShardPointer pointer) throws IOException {
            BytesReference payloadBR = new BytesArray(payload);
            Map<String, Object> payloadMap = XContentHelper.convertToMap(payloadBR, false, MediaTypeRegistry.xContentType(payloadBR)).v2();

            String id = (String) payloadMap.getOrDefault(ID, "null");
            if (payloadMap.containsKey(OP_TYPE) && !(payloadMap.get(OP_TYPE) instanceof String)) {
                // TODO: add metric
                logger.error("_op_type field is of type {} but not string, skipping the message", payloadMap.get(OP_TYPE).getClass());
                return null;
            }
            String opTypeString = (String) payloadMap.getOrDefault(OP_TYPE, "index");
            DocWriteRequest.OpType opType = DocWriteRequest.OpType.fromString(opTypeString);

            Engine.Operation operation;
            switch (opType) {
                case INDEX:
                    if (!payloadMap.containsKey(SOURCE)) {
                        // TODO: add metric
                        logger.error("missing _source field, skipping the message");
                        return null;
                    }
                    if (!(payloadMap.get(SOURCE) instanceof Map)) {
                        // TODO: add metric
                        logger.error("_source field does not contain a map, skipping the message");
                        return null;
                    }
                    BytesReference source = convertToBytes(payloadMap.get(SOURCE));

                    SourceToParse sourceToParse = new SourceToParse(index, id, source, MediaTypeRegistry.xContentType(source), null);
                    // TODO: handle parsing err
                    ParsedDocument doc = engine.getDocumentMapperForType().getDocumentMapper().parse(sourceToParse);
                    ParseContext.Document document = doc.rootDoc();
                    // set the offset as the offset field
                    document.add(pointer.asPointField(IngestionShardPointer.OFFSET_FIELD));
                    // store the offset as string in stored field
                    document.add(new StoredField(IngestionShardPointer.OFFSET_FIELD, pointer.asString()));

                    operation = new Engine.Index(
                        new Term("_id", id),
                        doc,
                        0,
                        1,
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        System.currentTimeMillis(),
                        false,
                        UNASSIGNED_SEQ_NO,
                        0
                    );
                    break;
                case DELETE:
                    operation = new Engine.Delete(
                        id,
                        new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
                        0,
                        1,
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        UNASSIGNED_SEQ_NO,
                        0
                    );
                    break;
                default:
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

    BlockingQueue<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> getBlockingQueue() {
        return blockingQueue;
    }

    @Override
    public void run() {
        while (!(Thread.currentThread().isInterrupted())) {
            IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message> result = null;
            try {
                result = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // TODO: add metric
                logger.debug("MessageProcessorRunnable poll interruptedException", e);
                Thread.currentThread().interrupt(); // Restore interrupt status
            }
            if (result != null) {
                messageProcessor.process(result.getMessage(), result.getPointer());
            }
        }
    }
}
