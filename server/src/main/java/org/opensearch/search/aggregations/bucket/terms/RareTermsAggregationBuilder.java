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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation Builder for rare_terms agg
 *
 * @opensearch.internal
 */
public class RareTermsAggregationBuilder extends ValuesSourceAggregationBuilder<RareTermsAggregationBuilder> {
    public static final String NAME = "rare_terms";
    public static final ValuesSourceRegistry.RegistryKey<RareTermsAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        RareTermsAggregatorSupplier.class
    );

    private static final ParseField MAX_DOC_COUNT_FIELD_NAME = new ParseField("max_doc_count");
    private static final ParseField PRECISION = new ParseField("precision");

    private static final int MAX_MAX_DOC_COUNT = 100;
    public static final ObjectParser<RareTermsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        RareTermsAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareLong(RareTermsAggregationBuilder::maxDocCount, MAX_DOC_COUNT_FIELD_NAME);

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
            IncludeExclude::parseInclude,
            IncludeExclude.INCLUDE_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
            IncludeExclude::parseExclude,
            IncludeExclude.EXCLUDE_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );

        PARSER.declareDouble(RareTermsAggregationBuilder::setPrecision, PRECISION);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        RareTermsAggregatorFactory.registerAggregators(builder);
    }

    private IncludeExclude includeExclude = null;
    private int maxDocCount = 1;
    private double precision = 0.001;

    public RareTermsAggregationBuilder(String name) {
        super(name);
    }

    private RareTermsAggregationBuilder(
        RareTermsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.includeExclude = clone.includeExclude;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RareTermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public RareTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        maxDocCount = in.readVInt();
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(includeExclude);
        out.writeVInt(maxDocCount);
    }

    /**
     * Set the maximum document count terms should have in order to appear in
     * the response.
     */
    public RareTermsAggregationBuilder maxDocCount(long maxDocCount) {
        if (maxDocCount <= 0) {
            throw new IllegalArgumentException(
                "["
                    + MAX_DOC_COUNT_FIELD_NAME.getPreferredName()
                    + "] must be greater than 0. Found ["
                    + maxDocCount
                    + "] in ["
                    + name
                    + "]"
            );
        }
        // TODO review: what size cap should we put on this?
        if (maxDocCount > MAX_MAX_DOC_COUNT) {
            throw new IllegalArgumentException(
                "[" + MAX_DOC_COUNT_FIELD_NAME.getPreferredName() + "] must be smaller" + "than " + MAX_MAX_DOC_COUNT + "in [" + name + "]"
            );
        }
        this.maxDocCount = (int) maxDocCount;
        return this;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     */
    public RareTermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    /**
     * Get the current false positive rate for individual cuckoo filters.
     */
    public double getPrecision() {
        return precision;
    }

    /**
     * Set's the false-positive rate for individual cuckoo filters.  Does not dictate the overall fpp rate
     * since we use a "scaling" cuckoo filter which adds more filters as required, and the overall
     * error rate grows differently than individual filters
     *
     * This value does, however, affect the overall space usage of the filter.  Coarser precisions provide
     * more compact filters.  The default is 0.01
     */
    public void setPrecision(double precision) {
        if (precision < 0.00001) {
            throw new IllegalArgumentException("[precision] must be greater than 0.00001");
        }
        this.precision = precision;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new RareTermsAggregatorFactory(
            name,
            config,
            includeExclude,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata,
            maxDocCount,
            precision
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        builder.field(MAX_DOC_COUNT_FIELD_NAME.getPreferredName(), maxDocCount);
        builder.field(PRECISION.getPreferredName(), precision);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), includeExclude, maxDocCount, precision);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        RareTermsAggregationBuilder other = (RareTermsAggregationBuilder) obj;
        return Objects.equals(includeExclude, other.includeExclude)
            && Objects.equals(maxDocCount, other.maxDocCount)
            && Objects.equals(precision, other.precision);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
