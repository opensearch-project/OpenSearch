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
 * Aggregation Builder for extended stats agg
 *
 * @opensearch.internal
 */
public class ExtendedStatsAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<
    ValuesSource.Numeric,
    ExtendedStatsAggregationBuilder> {
    public static final String NAME = "extended_stats";
    public static final ValuesSourceRegistry.RegistryKey<ExtendedStatsAggregatorProvider> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, ExtendedStatsAggregatorProvider.class);

    public static final ObjectParser<ExtendedStatsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        ExtendedStatsAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(ExtendedStatsAggregationBuilder::sigma, ExtendedStatsAggregator.SIGMA_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        ExtendedStatsAggregatorFactory.registerAggregators(builder);
    }

    private double sigma = 2.0;

    public ExtendedStatsAggregationBuilder(String name) {
        super(name);
    }

    protected ExtendedStatsAggregationBuilder(
        ExtendedStatsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.sigma = clone.sigma;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ExtendedStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ExtendedStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        sigma = in.readDouble();
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(sigma);
    }

    public ExtendedStatsAggregationBuilder sigma(double sigma) {
        if (sigma < 0.0) {
            throw new IllegalArgumentException("[sigma] must be greater than or equal to 0. Found [" + sigma + "] in [" + name + "]");
        }
        this.sigma = sigma;
        return this;
    }

    public double sigma() {
        return sigma;
    }

    @Override
    protected ExtendedStatsAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new ExtendedStatsAggregatorFactory(name, config, sigma, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ExtendedStatsAggregator.SIGMA_FIELD.getPreferredName(), sigma);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sigma);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ExtendedStatsAggregationBuilder other = (ExtendedStatsAggregationBuilder) obj;
        return Objects.equals(sigma, other.sigma);
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
