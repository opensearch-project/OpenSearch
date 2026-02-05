/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.HllFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} for HyperLogLog++ sketch fields.
 * This field type stores pre-aggregated cardinality data using HLL++ sketch data structures.
 * It is intended for internal use by OpenSearch and its plugins (such as ISM for multi-tier rollup).
 *
 * @opensearch.internal
 */
public class HllFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "hll";

    private static HllFieldMapper toType(FieldMapper in) {
        return (HllFieldMapper) in;
    }

    /**
     * Builder for the HLL field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Integer> precision = Parameter.intParam(
            "precision",
            false,
            m -> toType(m).precision,
            HyperLogLogPlusPlus.DEFAULT_PRECISION
        ).setValidator(Builder::validatePrecision);

        private static void validatePrecision(int precision) {
            if (precision < AbstractHyperLogLog.MIN_PRECISION || precision > AbstractHyperLogLog.MAX_PRECISION) {
                throw new IllegalArgumentException(
                    "precision must be between "
                        + AbstractHyperLogLog.MIN_PRECISION
                        + " and "
                        + AbstractHyperLogLog.MAX_PRECISION
                        + ", got: "
                        + precision
                );
            }
        }

        // HLL fields are always stored as doc values and cannot be indexed or stored separately
        private final Parameter<Boolean> index = Parameter.indexParam(m -> false, false).setValidator(v -> {
            if (v) {
                throw new MapperParsingException("Cannot set [index] on field of type [" + CONTENT_TYPE + "]");
            }
        });

        private final Parameter<Boolean> store = Parameter.storeParam(m -> false, false).setValidator(v -> {
            if (v) {
                throw new MapperParsingException("Cannot set [store] on field of type [" + CONTENT_TYPE + "]");
            }
        });

        // Doc values are always enabled for HLL fields (this is how the data is stored)
        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> true, true).setValidator(v -> {
            if (!v) {
                throw new MapperParsingException("Cannot disable [doc_values] on field of type [" + CONTENT_TYPE + "]");
            }
        });

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(precision, index, store, docValues, meta);
        }

        @Override
        public HllFieldMapper build(BuilderContext context) {
            return new HllFieldMapper(
                name,
                new HllFieldType(buildFullName(context), precision.getValue(), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                precision.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        // HLL fields are intended for internal use by OpenSearch and plugins only.
        return new Builder(n);
    });

    /**
     * HLL field type
     *
     * @opensearch.internal
     */
    public static final class HllFieldType extends MappedFieldType {

        private final int precision;

        public HllFieldType(String name, int precision, Map<String, String> meta) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
            this.precision = precision;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public int precision() {
            return precision;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            // Support derived source by fetching from doc values
            // This enables storage optimization when _source is disabled
            return new DocValueFetcher(docValueFormat(format, null), searchLookup.doc().getForField(this));
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            // HLL sketches are stored as binary data and formatted as base64
            return DocValueFormat.BINARY;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new HllFieldData.Builder(name(), precision);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Term queries are not supported on [hll] fields");
        }
    }

    private final int precision;

    private HllFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo, int precision) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.precision = precision;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // Parse binary HLL++ sketch data
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

        // Validate the sketch data
        BytesRef sketchBytes = new BytesRef(value);
        validateSketchData(sketchBytes);

        // Store as binary doc value
        context.doc().add(new BinaryDocValuesField(fieldType().name(), sketchBytes));
    }

    /**
     * Validates that the binary data is a valid HLL++ sketch by attempting to deserialize it.
     *
     * @param sketchBytes the binary sketch data to validate
     * @throws MapperParsingException if the data is not a valid HLL++ sketch
     */
    private void validateSketchData(BytesRef sketchBytes) throws MapperParsingException {
        try (StreamInput in = new BytesArray(sketchBytes.bytes, sketchBytes.offset, sketchBytes.length).streamInput()) {
            try (AbstractHyperLogLogPlusPlus sketch = AbstractHyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE)) {
                // Verify the precision matches the field's configured precision
                if (sketch.precision() != precision) {
                    throw new MapperParsingException(
                        "HLL++ sketch precision mismatch for field ["
                            + fieldType().name()
                            + "]: "
                            + "expected "
                            + precision
                            + ", got "
                            + sketch.precision()
                    );
                }
            }
        } catch (MapperParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new MapperParsingException("Invalid HLL++ sketch data for field [" + fieldType().name() + "]", e);
        }
    }

    @Override
    public HllFieldType fieldType() {
        return (HllFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
