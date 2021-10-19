/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.opensearch.cluster.metadata.DataStream.TimestampField;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DataStreamFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_data_stream_timestamp";
    public static final String CONTENT_TYPE = "_data_stream_timestamp";

    public static final class Defaults {
        public static final boolean ENABLED = false;
        public static final TimestampField TIMESTAMP_FIELD = new TimestampField("@timestamp");
    }

    public static final class Builder extends MetadataFieldMapper.Builder {
        final Parameter<Boolean> enabledParam = Parameter.boolParam("enabled", false, mapper -> toType(mapper).enabled, Defaults.ENABLED);

        final Parameter<TimestampField> timestampFieldParam = new Parameter<>(
            "timestamp_field",
            false,
            () -> Defaults.TIMESTAMP_FIELD,
            (n, c, o) -> new TimestampField((String) ((Map<?, ?>) o).get("name")),
            mapper -> toType(mapper).timestampField
        );

        protected Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.unmodifiableList(Arrays.asList(enabledParam, timestampFieldParam));
        }

        @Override
        public MetadataFieldMapper build(BuilderContext context) {
            return new DataStreamFieldMapper(enabledParam.getValue(), timestampFieldParam.getValue());
        }
    }

    public static final class DataStreamFieldType extends MappedFieldType {
        public static final DataStreamFieldType INSTANCE = new DataStreamFieldType();

        private DataStreamFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + typeName() + "]");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run term query on internal field [" + typeName() + "]");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on internal field [" + typeName() + "]");
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        context -> new DataStreamFieldMapper(Defaults.ENABLED, Defaults.TIMESTAMP_FIELD),
        context -> new Builder()
    );

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    private static DataStreamFieldMapper toType(FieldMapper in) {
        return (DataStreamFieldMapper) in;
    }

    private final boolean enabled;
    private final TimestampField timestampField;

    protected DataStreamFieldMapper(boolean enabled, TimestampField timestampField) {
        super(DataStreamFieldType.INSTANCE);
        this.enabled = enabled;
        this.timestampField = timestampField;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // If _data_stream_timestamp metadata mapping is disabled, then skip all the remaining checks.
        if (enabled == false) {
            return;
        }

        // It is expected that the timestamp field will be parsed by the DateFieldMapper during the parseCreateField step.
        // The parsed field will be added to the document as:
        // 1. LongPoint (indexed = true; an indexed long field to allow fast range filters on the timestamp field value)
        // 2. SortedNumericDocValuesField (hasDocValues = true; allows sorting, aggregations and access to the timestamp field value)

        Document document = context.doc();
        IndexableField[] fields = document.getFields(timestampField.getName());

        // Documents must contain exactly one value for the timestamp field.
        long numTimestampValues = Arrays.stream(fields)
            .filter(field -> field.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC)
            .count();

        if (numTimestampValues != 1) {
            throw new IllegalArgumentException(
                "documents must contain a single-valued timestamp field '" + timestampField.getName() + "' of date type"
            );
        }
    }
}
