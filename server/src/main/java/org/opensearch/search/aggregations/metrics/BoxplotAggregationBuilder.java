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
 * Aggregation builder for the boxplot aggregation, which computes the minimum, maximum, quartiles and
 * whiskers of the numeric values of the matching documents.
 *
 * @opensearch.internal
 */
public class BoxplotAggregationBuilder extends LeafOnly<ValuesSource.Numeric, BoxplotAggregationBuilder> {

    /**
     * The name of the aggregation, which is also its type in requests and responses.
     */
    public static final String NAME = InternalBoxplot.NAME;

    /**
     * The key under which the aggregators of this aggregation are registered for each values source type.
     */
    public static final ValuesSourceRegistry.RegistryKey<BoxplotAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        BoxplotAggregatorSupplier.class
    );

    private static final ParseField COMPRESSION_FIELD = new ParseField("compression");

    private static final double DEFAULT_COMPRESSION = 100d;

    /**
     * The largest accepted compression. The t-digest allocates memory in proportion to its compression when it is created, so a
     * bound keeps a single request from exhausting the heap.
     */
    public static final double MAX_COMPRESSION = 10_000d;

    /**
     * Parser of the aggregation from its request representation.
     */
    public static final ObjectParser<BoxplotAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        BoxplotAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(BoxplotAggregationBuilder::compression, COMPRESSION_FIELD);
    }

    /**
     * Registers the aggregators that compute this aggregation for each supported values source type.
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        BoxplotAggregatorFactory.registerAggregators(builder);
    }

    private double compression = DEFAULT_COMPRESSION;

    /**
     * Creates a boxplot aggregation with the given name.
     */
    public BoxplotAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Read from a stream.
     */
    public BoxplotAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        compression = in.readDouble();
    }

    /**
     * Creates a copy of the given aggregation with other sub-aggregations and metadata.
     */
    protected BoxplotAggregationBuilder(
        BoxplotAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.compression = clone.compression;
    }

    /**
     * Returns the compression factor of the t-digest sketches used. The default is {@code 100}.
     */
    public double compression() {
        return compression;
    }

    /**
     * Sets the compression factor of the t-digest sketches used. A higher compression is more accurate and
     * uses more memory.
     *
     * @throws IllegalArgumentException if the compression is not a number greater than zero, or is greater than
     *         {@link #MAX_COMPRESSION}
     */
    public BoxplotAggregationBuilder compression(double compression) {
        if ((compression > 0d) == false) {
            throw new IllegalArgumentException(
                "[" + COMPRESSION_FIELD.getPreferredName() + "] must be greater than 0. Found [" + compression + "] in [" + name + "]"
            );
        }
        if (compression > MAX_COMPRESSION) {
            throw new IllegalArgumentException(
                "["
                    + COMPRESSION_FIELD.getPreferredName()
                    + "] must be at most "
                    + MAX_COMPRESSION
                    + ". Found ["
                    + compression
                    + "] in ["
                    + name
                    + "]"
            );
        }
        this.compression = compression;
        return this;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new BoxplotAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(compression);
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new BoxplotAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata, compression);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(COMPRESSION_FIELD.getPreferredName(), compression);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), compression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BoxplotAggregationBuilder other = (BoxplotAggregationBuilder) obj;
        return Double.compare(compression, other.compression) == 0;
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
