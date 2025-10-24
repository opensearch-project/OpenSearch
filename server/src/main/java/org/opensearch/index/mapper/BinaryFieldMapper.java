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

package org.opensearch.index.mapper;

import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A mapper for binary fields
 *
 * @opensearch.internal
 */
public class BinaryFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "binary";

    private static BinaryFieldMapper toType(FieldMapper in) {
        return (BinaryFieldMapper) in;
    }

    /**
     * Builder for the binary field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, false);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            this(name, false);
        }

        public Builder(String name, boolean hasDocValues) {
            super(name);
            this.hasDocValues.setValue(hasDocValues);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(meta, stored, hasDocValues);
        }

        @Override
        public BinaryFieldMapper build(BuilderContext context) {
            return new BinaryFieldMapper(
                name,
                new BinaryFieldType(buildFullName(context), stored.getValue(), hasDocValues.getValue(), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * Binary field type
     *
     * @opensearch.internal
     */
    public static final class BinaryFieldType extends MappedFieldType {

        private BinaryFieldType(String name, boolean isStored, boolean hasDocValues, Map<String, String> meta) {
            super(name, false, isStored, hasDocValues, TextSearchInfo.NONE, meta);
        }

        public BinaryFieldType(String name) {
            this(name, false, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }

        @Override
        public BytesReference valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }

            BytesReference bytes;
            if (value instanceof BytesRef) {
                bytes = new BytesArray((BytesRef) value);
            } else if (value instanceof BytesReference) {
                bytes = (BytesReference) value;
            } else if (value instanceof byte[]) {
                bytes = new BytesArray((byte[]) value);
            } else {
                bytes = new BytesArray(Base64.getDecoder().decode(value.toString()));
            }
            return bytes;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new BytesBinaryIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Binary fields do not support searching");
        }
    }

    private final boolean stored;
    private final boolean hasDocValues;

    protected BinaryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (stored == false && hasDocValues == false) {
            return;
        }
        byte[] value = context.parseExternalValue(byte[].class);
        if (value == null) {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                value = context.parser().binaryValue();
            }
        }
        if (value == null) {
            return;
        }

        if (isPluggableDataFormatFeatureEnabled()) {
            context.compositeDocumentInput().addField(fieldType(), value);
        } else {
            if (stored) {
                context.doc().add(new StoredField(fieldType().name(), value));
            }

            if (hasDocValues) {
                CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context.doc().getByKey(fieldType().name());
                if (field == null) {
                    field = new CustomBinaryDocValuesField(fieldType().name(), value);
                    context.doc().addWithKey(fieldType().name(), field);
                } else {
                    field.add(value);
                }
            } else {
                // Only add an entry to the field names field if the field is stored
                // but has no doc values so exists query will work on a field with
                // no doc values
                createFieldNamesField(context);
            }
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new BinaryFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Custom binary doc values field for the binary field mapper
     *
     * @opensearch.internal
     */
    public static class CustomBinaryDocValuesField extends CustomDocValuesField {

        // We considered using a TreeSet instead of an ArrayList here.
        // Benchmarks show that ArrayList performs much better
        // For details, see: https://github.com/opensearch-project/OpenSearch/pull/9426
        // Benchmarks are in CustomBinaryDocValuesFiledBenchmark
        private final ArrayList<byte[]> bytesList;

        public CustomBinaryDocValuesField(String name, byte[] bytes) {
            super(name);
            bytesList = new ArrayList<>();
            add(bytes);
        }

        public void add(byte[] bytes) {
            bytesList.add(bytes);
        }

        @Override
        public BytesRef binaryValue() {
            try {
                // sort and dedup in place
                CollectionUtils.sortAndDedup(bytesList, Arrays::compareUnsigned);
                int size = bytesList.stream().map(b -> b.length).reduce(0, Integer::sum);
                int length = bytesList.size();
                try (BytesStreamOutput out = new BytesStreamOutput(size + (length + 1) * 5)) {
                    out.writeVInt(length);  // write total number of values
                    for (byte[] value : bytesList) {
                        int valueLength = value.length;
                        out.writeVInt(valueLength);
                        out.writeBytes(value, 0, valueLength);
                    }
                    return out.bytes().toBytesRef();
                }
            } catch (IOException e) {
                throw new OpenSearchException("Failed to get binary value", e);
            }

        }

        @Override
        public StoredValue storedValue() {
            return null;
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }
    }
}
