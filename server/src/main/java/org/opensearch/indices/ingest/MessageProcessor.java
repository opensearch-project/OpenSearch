/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.Term;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;

import java.io.IOException;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 *  A class to process messages from the ingestion stream. It extracts the payload from the message and creates an
 *  engine operation.
 */
public class MessageProcessor {
    private static final Logger logger = LogManager.getLogger(MessageProcessor.class);


    private final IngestionEngine engine;

    /**
     * Constructor.
     *
     * @param engine the ingestion engine
     */
    public MessageProcessor(IngestionEngine engine) {
        this.engine = engine;
    }


    /**
     * Process the message and create an engine operation. It also records the offset in the document as (1) a point
     * field used for range search, (2) a stored field for retrieval.
     *
     * @param message the message to process
     * @param pointer the pointer to the message
     */
    public void process(Message message, IngestionShardPointer pointer) {
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
            logger.error("Failed to process operation {} from message {}", operation, message, e);
            throw new RuntimeException(e);
        }
    }

    private Engine.Operation getOperation(byte[] payload, IngestionShardPointer pointer){
        // TODO: get id from the message
        String id = "null";
        BytesReference source = new BytesArray(payload);
        SourceToParse sourceToParse = new SourceToParse(
            "index",
            id,
            source,
            MediaTypeRegistry.xContentType(source),
            null
        );
        ParsedDocument doc = engine.getDocumentMapperForType().getDocumentMapper().parse(sourceToParse);
        for(ParseContext.Document document: doc.docs()){
            // set the offset as the offset field
            document.add(pointer.asPointField(IngestionShardPointer.OFFSET_FIELD));
            // store the offset as string in stored field
            document.add(new StoredField(IngestionShardPointer.OFFSET_FIELD,pointer.asString()));
        }
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
