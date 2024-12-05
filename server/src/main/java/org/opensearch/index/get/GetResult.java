/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.get;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Result provided for a search get
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetResult implements Writeable, Iterable<DocumentField>, ToXContentObject {

    public static final String _INDEX = "_index";
    public static final String _ID = "_id";
    private static final String _VERSION = "_version";
    private static final String _SEQ_NO = "_seq_no";
    private static final String _PRIMARY_TERM = "_primary_term";
    private static final String FOUND = "found";
    private static final String FIELDS = "fields";

    private String index;
    private String id;
    private long version;
    private long seqNo;
    private long primaryTerm;
    private boolean exists;
    private final Map<String, DocumentField> documentFields;
    private final Map<String, DocumentField> metaFields;
    private Map<String, Object> sourceAsMap;
    private BytesReference source;
    private byte[] sourceAsBytes;

    public GetResult(StreamInput in) throws IOException {
        index = in.readString();
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readOptionalString();
        }
        id = in.readString();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        version = in.readLong();
        exists = in.readBoolean();
        if (exists) {
            source = in.readBytesReference();
            if (source.length() == 0) {
                source = null;
            }
            documentFields = readFields(in);
            metaFields = readFields(in);
        } else {
            metaFields = Collections.emptyMap();
            documentFields = Collections.emptyMap();
        }
    }

    public GetResult(
        String index,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        boolean exists,
        BytesReference source,
        Map<String, DocumentField> documentFields,
        Map<String, DocumentField> metaFields
    ) {
        this.index = index;
        this.id = id;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        assert (seqNo == UNASSIGNED_SEQ_NO && primaryTerm == UNASSIGNED_PRIMARY_TERM) || (seqNo >= 0 && primaryTerm >= 1) : "seqNo: "
            + seqNo
            + " primaryTerm: "
            + primaryTerm;
        assert exists || (seqNo == UNASSIGNED_SEQ_NO && primaryTerm == UNASSIGNED_PRIMARY_TERM)
            : "doc not found but seqNo/primaryTerm are set";
        this.version = version;
        this.exists = exists;
        this.source = source;
        this.documentFields = documentFields == null ? emptyMap() : documentFields;
        this.metaFields = metaFields == null ? emptyMap() : metaFields;
    }

    /**
     * Does the document exist.
     */
    public boolean isExists() {
        return exists;
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return index;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id;
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return version;
    }

    /**
     * The sequence number assigned to the last operation that has changed this document, if found.
     */
    public long getSeqNo() {
        return seqNo;
    }

    /**
     * The primary term of the last primary that has changed this document, if found.
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * The source of the document if exists.
     */
    public byte[] source() {
        if (source == null) {
            return null;
        }
        if (sourceAsBytes != null) {
            return sourceAsBytes;
        }
        this.sourceAsBytes = BytesReference.toBytes(sourceRef());
        return this.sourceAsBytes;
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference sourceRef() {
        if (source == null) {
            return null;
        }

        try {
            this.source = CompressorRegistry.uncompressIfNeeded(this.source);
            return this.source;
        } catch (IOException e) {
            throw new OpenSearchParseException("failed to decompress source", e);
        }
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return source;
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return source == null;
    }

    /**
     * The source of the document (as a string).
     */
    public String sourceAsString() {
        if (source == null) {
            return null;
        }
        BytesReference source = sourceRef();
        try {
            return XContentHelper.convertToJson(source, false);
        } catch (IOException e) {
            throw new OpenSearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document (As a map).
     */
    public Map<String, Object> sourceAsMap() throws OpenSearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    public Map<String, DocumentField> getMetadataFields() {
        return metaFields;
    }

    public Map<String, DocumentField> getDocumentFields() {
        return documentFields;
    }

    public Map<String, DocumentField> getFields() {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.putAll(metaFields);
        fields.putAll(documentFields);
        return fields;
    }

    public DocumentField field(String name) {
        return getFields().get(name);
    }

    @Override
    public Iterator<DocumentField> iterator() {
        // need to join the fields and metadata fields
        Map<String, DocumentField> allFields = this.getFields();
        return allFields.values().iterator();
    }

    public XContentBuilder toXContentEmbedded(XContentBuilder builder, Params params) throws IOException {
        if (seqNo != UNASSIGNED_SEQ_NO) { // seqNo may not be assigned if read from an old node
            builder.field(_SEQ_NO, seqNo);
            builder.field(_PRIMARY_TERM, primaryTerm);
        }

        for (DocumentField field : metaFields.values()) {
            // TODO: can we avoid having an exception here?
            if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                builder.field(field.getName(), field.getValues());
            } else {
                builder.field(field.getName(), field.<Object>getValue());
            }
        }

        builder.field(FOUND, exists);

        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }

        if (!documentFields.isEmpty()) {
            builder.startObject(FIELDS);
            for (DocumentField field : documentFields.values()) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(_INDEX, index);
        builder.field(_ID, id);
        if (isExists()) {
            if (version != -1) {
                builder.field(_VERSION, version);
            }
            toXContentEmbedded(builder, params);
        } else {
            builder.field(FOUND, false);
        }
        builder.endObject();
        return builder;
    }

    public static GetResult fromXContentEmbedded(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return fromXContentEmbedded(parser, null, null);
    }

    public static GetResult fromXContentEmbedded(XContentParser parser, String index, String id) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        long version = -1;
        long seqNo = UNASSIGNED_SEQ_NO;
        long primaryTerm = UNASSIGNED_PRIMARY_TERM;
        Boolean found = null;
        BytesReference source = null;
        Map<String, DocumentField> documentFields = new HashMap<>();
        Map<String, DocumentField> metaFields = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (_INDEX.equals(currentFieldName)) {
                    index = parser.text();
                } else if (_ID.equals(currentFieldName)) {
                    id = parser.text();
                } else if (_VERSION.equals(currentFieldName)) {
                    version = parser.longValue();
                } else if (_SEQ_NO.equals(currentFieldName)) {
                    seqNo = parser.longValue();
                } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                    primaryTerm = parser.longValue();
                } else if (FOUND.equals(currentFieldName)) {
                    found = parser.booleanValue();
                } else {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, Collections.singletonList(parser.objectText())));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                    try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                        // the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        // it all depends on the current builder settings
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder);
                    }
                } else if (FIELDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        DocumentField getField = DocumentField.fromXContent(parser);
                        documentFields.put(getField.getName(), getField);
                    }
                } else {
                    parser.skipChildren(); // skip potential inner objects for forward compatibility
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (IgnoredFieldMapper.NAME.equals(currentFieldName)) {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, parser.list()));
                } else {
                    parser.skipChildren(); // skip potential inner arrays for forward compatibility
                }
            }
        }

        if (found == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "Missing required field [%s]", GetResult.FOUND)
            );
        }

        return new GetResult(index, id, seqNo, primaryTerm, version, found, source, documentFields, metaFields);
    }

    public static GetResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        return fromXContentEmbedded(parser);
    }

    private Map<String, DocumentField> readFields(StreamInput in) throws IOException {
        Map<String, DocumentField> fields;
        int size = in.readVInt();
        if (size == 0) {
            fields = emptyMap();
        } else {
            fields = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                DocumentField field = new DocumentField(in);
                fields.put(field.getName(), field);
            }
        }
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeLong(version);
        out.writeBoolean(exists);
        if (exists) {
            out.writeBytesReference(source);
            writeFields(out, documentFields);
            writeFields(out, metaFields);
        }
    }

    private void writeFields(StreamOutput out, Map<String, DocumentField> fields) throws IOException {
        if (fields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(fields.size());
            for (DocumentField field : fields.values()) {
                field.writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetResult getResult = (GetResult) o;
        return version == getResult.version
            && seqNo == getResult.seqNo
            && primaryTerm == getResult.primaryTerm
            && exists == getResult.exists
            && Objects.equals(index, getResult.index)
            && Objects.equals(id, getResult.id)
            && Objects.equals(documentFields, getResult.documentFields)
            && Objects.equals(metaFields, getResult.metaFields)
            && Objects.equals(sourceAsMap(), getResult.sourceAsMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, seqNo, primaryTerm, exists, index, id, documentFields, metaFields, sourceAsMap());
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
