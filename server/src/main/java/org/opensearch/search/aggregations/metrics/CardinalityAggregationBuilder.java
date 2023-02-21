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

package org.opensearch.search.aggregations.metrics;

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
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation Builder for cardinality agg
 *
 * @opensearch.internal
 */
public final class CardinalityAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<
    ValuesSource,
    CardinalityAggregationBuilder> {

    public static final String NAME = "cardinality";
    public static final ValuesSourceRegistry.RegistryKey<CardinalityAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, CardinalityAggregatorSupplier.class);

    private static final ParseField REHASH = new ParseField("rehash").withAllDeprecated("no replacement - values will always be rehashed");
    public static final ParseField PRECISION_THRESHOLD_FIELD = new ParseField("precision_threshold");

    public static final ObjectParser<CardinalityAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        CardinalityAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
        PARSER.declareLong(CardinalityAggregationBuilder::precisionThreshold, CardinalityAggregationBuilder.PRECISION_THRESHOLD_FIELD);
        PARSER.declareLong((b, v) -> {/*ignore*/}, REHASH);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        CardinalityAggregatorFactory.registerAggregators(builder);
    }

    private Long precisionThreshold = null;

    public CardinalityAggregationBuilder(String name) {
        super(name);
    }

    public CardinalityAggregationBuilder(
        CardinalityAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.precisionThreshold = clone.precisionThreshold;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    /**
     * Read from a stream.
     */
    public CardinalityAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            precisionThreshold = in.readLong();
        }
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CardinalityAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        boolean hasPrecisionThreshold = precisionThreshold != null;
        out.writeBoolean(hasPrecisionThreshold);
        if (hasPrecisionThreshold) {
            out.writeLong(precisionThreshold);
        }
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    /**
     * Set a precision threshold. Higher values improve accuracy but also
     * increase memory usage.
     */
    public CardinalityAggregationBuilder precisionThreshold(long precisionThreshold) {
        if (precisionThreshold < 0) {
            throw new IllegalArgumentException(
                "[precisionThreshold] must be greater than or equal to 0. Found [" + precisionThreshold + "] in [" + name + "]"
            );
        }
        this.precisionThreshold = precisionThreshold;
        return this;
    }

    /**
     * Get the precision threshold. Higher values improve accuracy but also
     * increase memory usage. Will return <code>null</code> if the
     * precisionThreshold has not been set yet.
     */
    public Long precisionThreshold() {
        return precisionThreshold;
    }

    @Override
    protected CardinalityAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new CardinalityAggregatorFactory(name, config, precisionThreshold, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (precisionThreshold != null) {
            builder.field(PRECISION_THRESHOLD_FIELD.getPreferredName(), precisionThreshold);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precisionThreshold);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        CardinalityAggregationBuilder other = (CardinalityAggregationBuilder) obj;
        return Objects.equals(precisionThreshold, other.precisionThreshold);
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
