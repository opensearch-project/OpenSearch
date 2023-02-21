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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Internal field mapper for storing source (and recovery source)
 *
 * @opensearch.internal
 */
public class SourceFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String CONTENT_TYPE = "_source";
    private final Function<Map<String, ?>, Map<String, Object>> filter;

    /**
     * Default parameters for source fields
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    private static SourceFieldMapper toType(FieldMapper in) {
        return (SourceFieldMapper) in;
    }

    /**
     * Builder for source fields
     *
     * @opensearch.internal
     */
    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Boolean> enabled = Parameter.boolParam("enabled", false, m -> toType(m).enabled, Defaults.ENABLED);
        private final Parameter<List<String>> includes = Parameter.stringArrayParam(
            "includes",
            false,
            m -> Arrays.asList(toType(m).includes),
            Collections.emptyList()
        );
        private final Parameter<List<String>> excludes = Parameter.stringArrayParam(
            "excludes",
            false,
            m -> Arrays.asList(toType(m).excludes),
            Collections.emptyList()
        );

        public Builder() {
            super(Defaults.NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(enabled, includes, excludes);
        }

        @Override
        public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(
                enabled.getValue(),
                includes.getValue().toArray(new String[0]),
                excludes.getValue().toArray(new String[0])
            );
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> new SourceFieldMapper(), c -> new Builder());

    /**
     * Field type for source field mapper
     *
     * @opensearch.internal
     */
    static final class SourceFieldType extends MappedFieldType {

        private SourceFieldType(boolean enabled) {
            super(NAME, false, enabled, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup lookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new QueryShardException(context, "The _source field is not searchable");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "The _source field is not searchable");
        }
    }

    private final boolean enabled;
    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;

    private SourceFieldMapper() {
        this(Defaults.ENABLED, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    private SourceFieldMapper(boolean enabled, String[] includes, String[] excludes) {
        super(new SourceFieldType(enabled));
        this.enabled = enabled;
        this.includes = includes;
        this.excludes = excludes;
        final boolean filtered = CollectionUtils.isEmpty(includes) == false || CollectionUtils.isEmpty(excludes) == false;
        this.filter = enabled && filtered ? XContentMapValues.filter(includes, excludes) : null;
        this.complete = enabled && CollectionUtils.isEmpty(includes) && CollectionUtils.isEmpty(excludes);
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        BytesReference originalSource = context.sourceToParse().source();
        XContentType contentType = context.sourceToParse().getXContentType();
        final BytesReference adaptedSource = applyFilters(originalSource, contentType);

        if (adaptedSource != null) {
            final BytesRef ref = adaptedSource.toBytesRef();
            context.doc().add(new StoredField(fieldType().name(), ref.bytes, ref.offset, ref.length));
        }

        if (originalSource != null && adaptedSource != originalSource) {
            // if we omitted source or modified it we add the _recovery_source to ensure we have it for ops based recovery
            BytesRef ref = originalSource.toBytesRef();
            context.doc().add(new StoredField(RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_NAME, 1));
        }
    }

    @Nullable
    public BytesReference applyFilters(@Nullable BytesReference originalSource, @Nullable XContentType contentType) throws IOException {
        if (enabled && originalSource != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            if (filter != null) {
                // we don't update the context source if we filter, we want to keep it as is...
                Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(originalSource, true, contentType);
                Map<String, Object> filteredSource = filter.apply(mapTuple.v2());
                BytesStreamOutput bStream = new BytesStreamOutput();
                XContentType actualContentType = mapTuple.v1();
                XContentBuilder builder = XContentFactory.contentBuilder(actualContentType, bStream).map(filteredSource);
                builder.close();
                return bStream.bytes();
            } else {
                return originalSource;
            }
        } else {
            return null;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }
}
