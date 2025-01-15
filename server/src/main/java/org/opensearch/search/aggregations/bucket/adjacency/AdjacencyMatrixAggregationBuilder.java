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

package org.opensearch.search.aggregations.bucket.adjacency;

import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregator.KeyedFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Aggregation Builder for adjacency_matrix agg
 *
 * @opensearch.internal
 */
public class AdjacencyMatrixAggregationBuilder extends AbstractAggregationBuilder<AdjacencyMatrixAggregationBuilder> {
    public static final String NAME = "adjacency_matrix";

    private static final String DEFAULT_SEPARATOR = "&";

    private static final ParseField SEPARATOR_FIELD = new ParseField("separator");
    private static final ParseField FILTERS_FIELD = new ParseField("filters");
    private static final ParseField SHOW_ONLY_INTERSECTING = new ParseField("show_only_intersecting");

    private List<KeyedFilter> filters;
    private boolean showOnlyIntersecting = false;
    private String separator = DEFAULT_SEPARATOR;

    private static final ObjectParser<AdjacencyMatrixAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        AdjacencyMatrixAggregationBuilder::new
    );
    static {
        PARSER.declareString(AdjacencyMatrixAggregationBuilder::separator, SEPARATOR_FIELD);
        PARSER.declareNamedObjects(AdjacencyMatrixAggregationBuilder::setFiltersAsList, KeyedFilter.PARSER, FILTERS_FIELD);
        PARSER.declareBoolean(
            AdjacencyMatrixAggregationBuilder::setShowOnlyIntersecting,
            AdjacencyMatrixAggregationBuilder.SHOW_ONLY_INTERSECTING
        );
    }

    public static AggregationBuilder parse(XContentParser parser, String name) throws IOException {
        AdjacencyMatrixAggregationBuilder result = PARSER.parse(parser, name);
        result.checkConsistency();
        return result;
    }

    /**
     * @param name
     *            the name of this aggregation
     */
    protected AdjacencyMatrixAggregationBuilder(String name) {
        super(name);
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param filters
     *            the filters and their keys to use with this aggregation.
     */
    public AdjacencyMatrixAggregationBuilder(String name, Map<String, QueryBuilder> filters) {
        this(name, DEFAULT_SEPARATOR, filters);
    }

    protected AdjacencyMatrixAggregationBuilder(
        AdjacencyMatrixAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.filters = new ArrayList<>(clone.filters);
        this.separator = clone.separator;
        this.showOnlyIntersecting = clone.showOnlyIntersecting;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new AdjacencyMatrixAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param separator
     *            the string used to separate keys in intersections buckets e.g.
     *            &amp; character for keyed filters A and B would return an
     *            intersection bucket named A&amp;B
     * @param filters
     *            the filters and their key to use with this aggregation.
     */
    public AdjacencyMatrixAggregationBuilder(String name, String separator, Map<String, QueryBuilder> filters) {
        super(name);
        this.separator = separator;
        setFiltersAsMap(filters);
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param filters
     *            the filters and their key to use with this aggregation.
     * @param showOnlyIntersecting
     *            show only the buckets that intersection multiple documents
     */
    public AdjacencyMatrixAggregationBuilder(String name, Map<String, QueryBuilder> filters, boolean showOnlyIntersecting) {
        this(name, DEFAULT_SEPARATOR, filters, showOnlyIntersecting);
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param separator
     *            the string used to separate keys in intersections buckets e.g.
     *            &amp; character for keyed filters A and B would return an
     *            intersection bucket named A&amp;B
     * @param filters
     *            the filters and their key to use with this aggregation.
     * @param showOnlyIntersecting
     *            show only the buckets that intersection multiple documents
     */
    public AdjacencyMatrixAggregationBuilder(
        String name,
        String separator,
        Map<String, QueryBuilder> filters,
        boolean showOnlyIntersecting
    ) {
        this(name, separator, filters);
        this.showOnlyIntersecting = showOnlyIntersecting;
    }

    /**
     * Read from a stream.
     */
    public AdjacencyMatrixAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        int filtersSize = in.readVInt();
        separator = in.readString();
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            showOnlyIntersecting = in.readBoolean();
        }
        filters = new ArrayList<>(filtersSize);
        for (int i = 0; i < filtersSize; i++) {
            filters.add(new KeyedFilter(in));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(filters.size());
        out.writeString(separator);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeBoolean(showOnlyIntersecting);
        }
        for (KeyedFilter keyedFilter : filters) {
            keyedFilter.writeTo(out);
        }
    }

    private void checkConsistency() {
        if ((filters == null) || (filters.size() == 0)) {
            throw new IllegalStateException("[" + name + "] is missing : " + FILTERS_FIELD.getPreferredName() + " parameter");
        }
    }

    private void setFiltersAsMap(Map<String, QueryBuilder> filters) {
        // Convert uniquely named objects into internal KeyedFilters
        this.filters = new ArrayList<>(filters.size());
        for (Entry<String, QueryBuilder> kv : filters.entrySet()) {
            this.filters.add(new KeyedFilter(kv.getKey(), kv.getValue()));
        }
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        Collections.sort(this.filters, Comparator.comparing(KeyedFilter::key));
    }

    private AdjacencyMatrixAggregationBuilder setFiltersAsList(List<KeyedFilter> filters) {
        this.filters = new ArrayList<>(filters);
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        Collections.sort(this.filters, Comparator.comparing(KeyedFilter::key));
        return this;
    }

    public AdjacencyMatrixAggregationBuilder setShowOnlyIntersecting(boolean showOnlyIntersecting) {
        this.showOnlyIntersecting = showOnlyIntersecting;
        return this;
    }

    /**
     * Set the separator used to join pairs of bucket keys
     */
    public AdjacencyMatrixAggregationBuilder separator(String separator) {
        if (separator == null) {
            throw new IllegalArgumentException("[separator] must not be null: [" + name + "]");
        }
        this.separator = separator;
        return this;
    }

    /**
     * Get the separator used to join pairs of bucket keys
     */
    public String separator() {
        return separator;
    }

    /**
     * Get the filters. This will be an unmodifiable map
     */
    public Map<String, QueryBuilder> filters() {
        Map<String, QueryBuilder> result = new HashMap<>(this.filters.size());
        for (KeyedFilter keyedFilter : this.filters) {
            result.put(keyedFilter.key(), keyedFilter.filter());
        }
        return result;
    }

    public boolean isShowOnlyIntersecting() {
        return showOnlyIntersecting;
    }

    @Override
    protected AdjacencyMatrixAggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        boolean modified = false;
        List<KeyedFilter> rewrittenFilters = new ArrayList<>(filters.size());
        for (KeyedFilter kf : filters) {
            QueryBuilder rewritten = Rewriteable.rewrite(kf.filter(), queryShardContext);
            modified = modified || rewritten != kf.filter();
            rewrittenFilters.add(new KeyedFilter(kf.key(), rewritten));
        }
        if (modified) {
            return new AdjacencyMatrixAggregationBuilder(name).separator(separator)
                .setFiltersAsList(rewrittenFilters)
                .setShowOnlyIntersecting(showOnlyIntersecting);
        }
        return this;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        int maxFilters = queryShardContext.getIndexSettings().getMaxAdjacencyMatrixFilters();
        if (filters.size() > maxFilters) {
            throw new IllegalArgumentException(
                "Number of filters is too large, must be less than or equal to: ["
                    + maxFilters
                    + "] but was ["
                    + filters.size()
                    + "]."
                    + "This limit can be set by changing the ["
                    + IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()
                    + "] index level setting."
            );
        }
        return new AdjacencyMatrixAggregatorFactory(
            name,
            filters,
            showOnlyIntersecting,
            separator,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SEPARATOR_FIELD.getPreferredName(), separator);
        builder.field(SHOW_ONLY_INTERSECTING.getPreferredName(), showOnlyIntersecting);
        builder.startObject(FILTERS_FIELD.getPreferredName());
        for (KeyedFilter keyedFilter : filters) {
            builder.field(keyedFilter.key(), keyedFilter.filter());
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filters, showOnlyIntersecting, separator);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        AdjacencyMatrixAggregationBuilder other = (AdjacencyMatrixAggregationBuilder) obj;
        return Objects.equals(filters, other.filters)
            && Objects.equals(separator, other.separator)
            && Objects.equals(showOnlyIntersecting, other.showOnlyIntersecting);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
