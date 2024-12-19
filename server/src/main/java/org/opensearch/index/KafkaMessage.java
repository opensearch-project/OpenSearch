/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.Term;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class KafkaMessage implements Message<String> {
    // TODO: support kafka header
    private final String key;
    private final String payload;
    // FIXME: how to make this generic outside of kafka message?
    private final KafkaOffset offset;

    public KafkaMessage(String key, String payload, KafkaOffset offset) {
        this.key = key;
        this.payload = payload;
        this.offset = offset;
    }


    public String getKey() {
        return key;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    @Override
    public Engine.Operation getOperation(DocumentMapperForType documentMapperForType) {
        // TODO: decode the bytes to get the operation
        String id = key == null ? "null" : key;
        BytesReference source = new BytesArray(payload);
        SourceToParse sourceToParse = new SourceToParse(
            "index",
            id,
            source,
            MediaTypeRegistry.xContentType(source),
            null
        );
        ParsedDocument doc = documentMapperForType.getDocumentMapper().parse(sourceToParse);
        for(ParseContext.Document document: doc.docs()){
            // set the offset as the offset field
            document.add(new NumericDocValuesField("_offset", offset.getOffset()));
        }
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
