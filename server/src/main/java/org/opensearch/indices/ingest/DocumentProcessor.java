/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

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

public class DocumentProcessor {

    private final IngestionEngine engine;

    public DocumentProcessor(IngestionEngine engine) {
        this.engine = engine;
    }


    public void process(Message message, IngestionShardPointer pointer) {
        // todo: support other types of payload
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
            // better error handling
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
            // TODO: make field name configurable?
            document.add(pointer.asPointField(IngestionShardPointer.OFFSET_FIELD));
            // store the offset as string in stored field
            document.add(new StoredField(IngestionShardPointer.OFFSET_FIELD,pointer.asString()));
        }
        // todo: support other types of operations than index
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
