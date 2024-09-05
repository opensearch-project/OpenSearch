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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
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
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SourceFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String CONTENT_TYPE = "_source";
    private final Function<Map<String, ?>, Map<String, Object>> filter;
    private final Function<Map<String, ?>, Map<String, Object>> recoverySourceFilter;

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

        /**
         * A mapping parameter which define whether the recovery_source should be added or not. Default value is true.
         * <p>
         * Recovery source gets added if source is disabled or there are filters that are applied on _source using
         * {@link #includes}/{@link #excludes}, which has the possibility to change the original document provided by
         * customer. Recovery source is not a permanent field and gets removed during merges. Refer this merge
         * policy: org.opensearch.index.engine.RecoverySourcePruneMergePolicy
         * <p>
         * The main reason for adding the _recovery_source was to ensure Peer to Peer recovery if segments
         * are not flushed to the disk. If you are disabling the recovery source, then ensure that you are calling
         * flush operation of Opensearch periodically to ensure that segments are flushed to the disk and if required
         * Peer to Peer recovery can happen using segment files rather than replaying traffic by querying Lucene
         * snapshot.
         *
         * <p>
         * This is an expert mapping parameter.
         *
         */
        private final Parameter<Boolean> recoverySourceEnabled = Parameter.boolParam(
            "recovery_source_enabled",
            false,
            m -> toType(m).recoverySourceEnabled,
            Defaults.ENABLED
        );

        /**
         * Provides capability to add specific fields in the recovery_source.
         * <p>
         * Refer {@link #recoverySourceEnabled} for more details
         * This is an expert parameter.
         */
        private final Parameter<List<String>> recoverySourceIncludes = Parameter.stringArrayParam(
            "recovery_source_includes",
            false,
            m -> Arrays.asList(toType(m).recoverySourceIncludes),
            Collections.emptyList()
        );

        /**
         * Provides capability to remove specific fields in the recovery_source.
         *
         * Refer {@link #recoverySourceEnabled} for more details
         * This is an expert parameter.
         */
        private final Parameter<List<String>> recoverySourceExcludes = Parameter.stringArrayParam(
            "recovery_source_excludes",
            false,
            m -> Arrays.asList(toType(m).recoverySourceExcludes),
            Collections.emptyList()
        );

        public Builder() {
            super(Defaults.NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(enabled, includes, excludes, recoverySourceEnabled, recoverySourceIncludes, recoverySourceExcludes);
        }

        @Override
        public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(
                enabled.getValue(),
                includes.getValue().toArray(new String[0]),
                excludes.getValue().toArray(new String[0]),
                recoverySourceEnabled.getValue(),
                recoverySourceIncludes.getValue().toArray(new String[0]),
                recoverySourceExcludes.getValue().toArray(new String[0])
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
    private final boolean recoverySourceEnabled;
    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;
    private final String[] recoverySourceIncludes;
    private final String[] recoverySourceExcludes;

    private SourceFieldMapper() {
        this(Defaults.ENABLED, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY, Defaults.ENABLED, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    private SourceFieldMapper(
        boolean enabled,
        String[] includes,
        String[] excludes,
        boolean recoverySourceEnabled,
        String[] recoverySourceIncludes,
        String[] recoverySourceExcludes
    ) {
        super(new SourceFieldType(enabled));
        this.enabled = enabled;
        this.includes = includes;
        this.excludes = excludes;
        final boolean filtered = CollectionUtils.isEmpty(includes) == false || CollectionUtils.isEmpty(excludes) == false;
        this.filter = enabled && filtered ? XContentMapValues.filter(includes, excludes) : null;
        this.complete = enabled && CollectionUtils.isEmpty(includes) && CollectionUtils.isEmpty(excludes);

        // Set parameters for recovery source
        this.recoverySourceEnabled = recoverySourceEnabled;
        this.recoverySourceIncludes = recoverySourceIncludes;
        this.recoverySourceExcludes = recoverySourceExcludes;
        final boolean recoverySourcefiltered = CollectionUtils.isEmpty(recoverySourceIncludes) == false
            || CollectionUtils.isEmpty(recoverySourceExcludes) == false;
        this.recoverySourceFilter = this.recoverySourceEnabled && recoverySourcefiltered
            ? XContentMapValues.filter(recoverySourceIncludes, recoverySourceExcludes)
            : null;
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
        MediaType contentType = context.sourceToParse().getMediaType();
        final BytesReference adaptedSource = applyFilters(originalSource, contentType);

        if (adaptedSource != null) {
            final BytesRef ref = adaptedSource.toBytesRef();
            context.doc().add(new StoredField(fieldType().name(), ref.bytes, ref.offset, ref.length));
        }

        if (recoverySourceEnabled) {
            if (originalSource != null && adaptedSource != originalSource) {
                final BytesReference adaptedRecoverySource = applyFilters(
                    originalSource,
                    contentType,
                    recoverySourceEnabled,
                    recoverySourceFilter
                );
                // if we omitted source or modified it we add the _recovery_source to ensure we have it for ops based recovery
                BytesRef ref = adaptedRecoverySource.toBytesRef();
                context.doc().add(new StoredField(RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
                context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_NAME, 1));
            }
        }
    }

    @Nullable
    public BytesReference applyFilters(@Nullable BytesReference originalSource, @Nullable MediaType contentType) throws IOException {
        return applyFilters(originalSource, contentType, enabled, filter);
    }

    @Nullable
    private BytesReference applyFilters(
        @Nullable BytesReference originalSource,
        @Nullable MediaType contentType,
        boolean isProvidedSourceEnabled,
        @Nullable final Function<Map<String, ?>, Map<String, Object>> filters
    ) throws IOException {
        if (isProvidedSourceEnabled && originalSource != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            if (filters != null) {
                // we don't update the context source if we filter, we want to keep it as is...
                Tuple<? extends MediaType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(originalSource, true, contentType);
                Map<String, Object> filteredSource = filters.apply(mapTuple.v2());
                BytesStreamOutput bStream = new BytesStreamOutput();
                MediaType actualContentType = mapTuple.v1();
                XContentBuilder builder = MediaTypeRegistry.contentBuilder(actualContentType, bStream).map(filteredSource);
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
