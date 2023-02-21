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

import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.opensearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation Builder for weighted_avg agg
 *
 * @opensearch.internal
 */
public class WeightedAvgAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<WeightedAvgAggregationBuilder> {
    public static final String NAME = "weighted_avg";
    public static final ParseField VALUE_FIELD = new ParseField("value");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");

    public static final ObjectParser<WeightedAvgAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        WeightedAvgAggregationBuilder::new
    );
    static {
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(VALUE_FIELD.getPreferredName(), PARSER, true, false, false);
        MultiValuesSourceParseHelper.declareField(WEIGHT_FIELD.getPreferredName(), PARSER, true, false, false);
    }

    public static void registerUsage(ValuesSourceRegistry.Builder builder) {
        builder.registerUsage(NAME, CoreValuesSourceType.NUMERIC);
    }

    public WeightedAvgAggregationBuilder(String name) {
        super(name);
    }

    public WeightedAvgAggregationBuilder(WeightedAvgAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    public WeightedAvgAggregationBuilder value(MultiValuesSourceFieldConfig valueConfig) {
        valueConfig = Objects.requireNonNull(valueConfig, "Configuration for field [" + VALUE_FIELD + "] cannot be null");
        field(VALUE_FIELD.getPreferredName(), valueConfig);
        return this;
    }

    public WeightedAvgAggregationBuilder weight(MultiValuesSourceFieldConfig weightConfig) {
        weightConfig = Objects.requireNonNull(weightConfig, "Configuration for field [" + WEIGHT_FIELD + "] cannot be null");
        field(WEIGHT_FIELD.getPreferredName(), weightConfig);
        return this;
    }

    /**
     * Read from a stream.
     */
    public WeightedAvgAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new WeightedAvgAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected MultiValuesSourceAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        Map<String, ValuesSourceConfig> configs,
        Map<String, QueryBuilder> filters,
        DocValueFormat format,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        return new WeightedAvgAggregatorFactory(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
