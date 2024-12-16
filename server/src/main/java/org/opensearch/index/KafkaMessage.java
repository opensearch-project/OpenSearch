/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.*;

import java.nio.charset.Charset;
import java.util.Arrays;

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
    public Engine.Operation getOperation() {
        // TODO: decode the bytes to get the operation
        ParsedDocument doc = toParsedDocument(
            key,
            null,
            testDocumentWithTextField("test"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );
        Engine.Index index = new Engine.Index(
            new Term("_id", key),
            doc,
            offset.toSequenceNumber(),
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
        // set the offset as the sequence number
        index.parsedDoc().updateSeqID(offset.toSequenceNumber(), 1);
        return index;
    }

    ParseContext.Document testDocumentWithTextField(String value) {
        ParseContext.Document document = new ParseContext.Document();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }

    private ParsedDocument toParsedDocument(String id,
                                              String routing,
                                              ParseContext.Document document,
                                              BytesReference source,
                                              Mapping mappingUpdate) {
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesRef ref = source.toBytesRef();
        document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        return new ParsedDocument(versionField, seqID, id, routing, Arrays.asList(document), source, MediaTypeRegistry.JSON, mappingUpdate);
    }
}


//public class KafkaMessage implements Message<byte[]> {
//    // TODO: support kafka header
//    private final byte[] key;
//    private final byte[] payload;
//
//    public KafkaMessage(byte[] key, byte[] payload) {
//        this.key = key;
//        this.payload = payload;
//    }
//
//
//    public byte[] getKey() {
//        return key;
//    }
//
//    @Override
//    public byte[] getPayload() {
//        return payload;
//    }
//
//    @Override
//    public Engine.Operation getOperation() {
//        // TODO: decode the bytes to get the operation
//        throw new UnsupportedOperationException();
//    }
//}
