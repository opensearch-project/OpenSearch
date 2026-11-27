/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder.LeafOnly;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation builder for the string_stats aggregation, which computes the number, the lengths, and the Shannon entropy of the
 * characters of the string values of the matching documents, and optionally the frequency of each character.
 *
 * @opensearch.internal
 */
public class StringStatsAggregationBuilder extends LeafOnly<ValuesSource.Bytes, StringStatsAggregationBuilder> {

    /**
     * The name of the aggregation, which is also its type in requests and responses.
     */
    public static final String NAME = InternalStringStats.NAME;

    /**
     * The key under which the aggregators of this aggregation are registered for each values source type.
     */
    public static final ValuesSourceRegistry.RegistryKey<StringStatsAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, StringStatsAggregatorSupplier.class);

    private static final ParseField SHOW_DISTRIBUTION_FIELD = new ParseField("show_distribution");

    /**
     * Parser of the aggregation from its request representation.
     */
    public static final ObjectParser<StringStatsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        StringStatsAggregationBuilder::new
    );
    static {
        // a script is supported, and a format is not as the values are strings, and neither is a time zone
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
        PARSER.declareBoolean(StringStatsAggregationBuilder::showDistribution, SHOW_DISTRIBUTION_FIELD);
    }

    /**
     * Registers the aggregators that compute this aggregation for each supported values source type.
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        StringStatsAggregatorFactory.registerAggregators(builder);
    }

    private boolean showDistribution = false;

    /**
     * Creates a string_stats aggregation with the given name.
     */
    public StringStatsAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Read from a stream.
     */
    public StringStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        showDistribution = in.readBoolean();
    }

    /**
     * Creates a copy of the given aggregation with other sub-aggregations and metadata.
     */
    protected StringStatsAggregationBuilder(
        StringStatsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.showDistribution = clone.showDistribution;
    }

    /**
     * Returns whether the response has the relative frequency of every character. The default is {@code false}.
     */
    public boolean showDistribution() {
        return showDistribution;
    }

    /**
     * Sets whether the response has the relative frequency of every character, which makes it as large as the number of distinct
     * characters of the values.
     */
    public StringStatsAggregationBuilder showDistribution(boolean showDistribution) {
        this.showDistribution = showDistribution;
        return this;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new StringStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalStringStats.ensureSupportedBy(out);
        out.writeBoolean(showDistribution);
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new StringStatsAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata, showDistribution);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(SHOW_DISTRIBUTION_FIELD.getPreferredName(), showDistribution);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), showDistribution);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        StringStatsAggregationBuilder other = (StringStatsAggregationBuilder) obj;
        return showDistribution == other.showDistribution;
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
