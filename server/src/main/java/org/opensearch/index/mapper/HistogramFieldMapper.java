package org.opensearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class HistogramFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "histogram";
    public static final String VALUES_FIELD = "values";
    public static final String COUNTS_FIELD = "counts";

    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
        FIELD_TYPE.freeze();
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(docValues, indexed, stored, meta);
        }

        @Override
        public HistogramFieldMapper build(BuilderContext context) {
            return new HistogramFieldMapper(
                name,
                new HistogramFieldType(buildFullName(context), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                indexed.getValue(),
                docValues.getValue(),
                stored.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;

    protected HistogramFieldMapper(String simpleName,
                                   MappedFieldType mappedFieldType,
                                   MultiFields multiFields,
                                   CopyTo copyTo,
                                   boolean indexed,
                                   boolean hasDocValues,
                                   boolean stored) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.indexed = indexed;
        this.hasDocValues = hasDocValues;
        this.stored = stored;
    }

    private static HistogramFieldMapper toType(FieldMapper mapper) {
        return (HistogramFieldMapper) mapper;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (!hasDocValues) {
            return;
        }

        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();

        if (token == XContentParser.Token.VALUE_NULL) {
            return;
        }

        List<Double> values = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (VALUES_FIELD.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            values.add(parser.doubleValue());
                        } else {
                            throw new IllegalArgumentException("values must be numbers");
                        }
                    }
                } else if (COUNTS_FIELD.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            // Check if the number is a decimal
                            if (parser.numberType() == XContentParser.NumberType.FLOAT
                                || parser.numberType() == XContentParser.NumberType.DOUBLE) {
                                throw new IllegalArgumentException("counts must be integers");
                            }
                            counts.add(parser.longValue());
                        } else {
                            throw new IllegalArgumentException("counts must be numbers");
                        }
                    }
                }
            }
        }

        if (values.isEmpty()) {
            throw new IllegalArgumentException("values array cannot be empty");
        }
        if (counts.isEmpty()) {
            throw new IllegalArgumentException("counts array cannot be empty");
        }

        validateHistogramData(values, counts);

        byte[] encodedData = encodeHistogram(values, counts);
        context.doc().add(new BinaryDocValuesField(fieldType().name(), new BytesRef(encodedData)));
    }


    private void validateHistogramData(List<Double> values, List<Long> counts) {
        if (values.size() != counts.size()) {
            throw new IllegalArgumentException("values and counts arrays must have the same length");
        }

        double previousValue = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < values.size(); i++) {
            double value = values.get(i);
            long count = counts.get(i);

            if (value <= previousValue) {
                throw new IllegalArgumentException("values must be in strictly ascending order");
            }

            if (count < 0) {
                throw new IllegalArgumentException("counts must be non-negative");
            }

            previousValue = value;
        }
    }


    private byte[] encodeHistogram(List<Double> values, List<Long> counts) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + (values.size() * (Double.BYTES + Long.BYTES)));
        buffer.putInt(values.size());

        for (int i = 0; i < values.size(); i++) {
            buffer.putDouble(values.get(i));
            buffer.putLong(counts.get(i));
        }

        return buffer.array();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class HistogramFieldType extends MappedFieldType {

        public HistogramFieldType(String name, Map<String, String> meta) {
            super(name, true, false, true, TextSearchInfo.NONE, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("Histogram fields do not support term queries");
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return new SourceValueFetcher(name(), context, format) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
            }
            return DocValueFormat.BINARY;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new BytesBinaryIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }
    }
}
