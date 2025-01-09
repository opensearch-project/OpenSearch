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
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;

import java.io.IOException;
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

        MessageProcessor(IngestionEngine engine) {
            this.engine = engine;
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

            Engine.Operation operation = getOperation(payload, pointer);
            try {
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
                logger.error("Failed to process operation {} from message {}: {}", operation, message, e);
                throw new RuntimeException(e);
            }
        }

        /**
         * Visible for testing. Get the engine operation from the message.
         * @param payload the payload of the message
         * @param pointer the pointer to the message
         * @return the engine operation
         */
        protected Engine.Operation getOperation(byte[] payload, IngestionShardPointer pointer) {
            // TODO: get id from the message
            String id = "null";
            BytesReference source = new BytesArray(payload);
            // TODO: parse the content to map to parse the inbuilt fields
//            XContentHelper.convertToMap()
            SourceToParse sourceToParse = new SourceToParse("index", id, source, MediaTypeRegistry.xContentType(source), null);
            ParsedDocument doc = engine.getDocumentMapperForType().getDocumentMapper().parse(sourceToParse);
            // FIXME: just add to root doc
            ParseContext.Document document = doc.rootDoc();
            // set the offset as the offset field
            document.add(pointer.asPointField(IngestionShardPointer.OFFSET_FIELD));
            // store the offset as string in stored field
            document.add(new StoredField(IngestionShardPointer.OFFSET_FIELD, pointer.asString()));
            // TODO: support delete
            Engine.Index index = new Engine.Index(
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

            return index;
        }
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
