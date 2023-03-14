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

package org.opensearch.geo.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geo.search.aggregations.bucket.geogrid.cells.CellIdSource;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceParserHelper;
import org.opensearch.search.aggregations.bucket.GeoTileUtils;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

/**
 * Builds values source for geotile_grid agg
 *
 * @opensearch.internal
 */
public class GeoTileGridValuesSourceBuilder extends CompositeValuesSourceBuilder<GeoTileGridValuesSourceBuilder> {
    /**
     * Supplier for a composite geotile
     *
     * @opensearch.internal
     */
    @FunctionalInterface
    public interface GeoTileCompositeSuppier {
        CompositeValuesSourceConfig apply(
            ValuesSourceConfig config,
            int precision,
            GeoBoundingBox boundingBox,
            String name,
            boolean hasScript, // probably redundant with the config, but currently we check this two different ways...
            String format,
            boolean missingBucket,
            MissingOrder missingOrder,
            SortOrder order
        );
    }

    public static final String TYPE = "geotile_grid";
    /*
        use the TYPE parameter instead of Byte code. The byte code is added for backward compatibility and will be
        removed in the next version.
     */
    @Deprecated
    public static final Byte COMPOSITE_AGGREGATION_SERIALISATION_BYTE_CODE = 3;
    static final ValuesSourceRegistry.RegistryKey<GeoTileCompositeSuppier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey(
        TYPE,
        GeoTileCompositeSuppier.class
    );

    static final ObjectParser<GeoTileGridValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoTileGridValuesSourceBuilder.TYPE);
        PARSER.declareInt(GeoTileGridValuesSourceBuilder::precision, new ParseField("precision"));
        PARSER.declareField(
            ((p, builder, context) -> builder.geoBoundingBox(GeoBoundingBox.parseBoundingBox(p))),
            GeoBoundingBox.BOUNDS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER);
    }

    public static GeoTileGridValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoTileGridValuesSourceBuilder(name), null);
    }

    public static void register(ValuesSourceRegistry.Builder builder) {

        builder.register(
            REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (valuesSourceConfig, precision, boundingBox, name, hasScript, format, missingBucket, missingOrder, order) -> {
                ValuesSource.GeoPoint geoPoint = (ValuesSource.GeoPoint) valuesSourceConfig.getValuesSource();
                // is specified in the builder.
                final MappedFieldType fieldType = valuesSourceConfig.fieldType();
                CellIdSource cellIdSource = new CellIdSource(geoPoint, precision, boundingBox, GeoTileUtils::longEncode);
                return new CompositeValuesSourceConfig(
                    name,
                    fieldType,
                    cellIdSource,
                    DocValueFormat.GEOTILE,
                    order,
                    missingBucket,
                    missingOrder,
                    hasScript,
                    (
                        BigArrays bigArrays,
                        IndexReader reader,
                        int size,
                        LongConsumer addRequestCircuitBreakerBytes,
                        CompositeValuesSourceConfig compositeValuesSourceConfig

                    ) -> {
                        final CellIdSource cis = (CellIdSource) compositeValuesSourceConfig.valuesSource();
                        return new GeoTileValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            cis::longValues,
                            LongUnaryOperator.identity(),
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            compositeValuesSourceConfig.missingOrder(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    }
                );
            },
            false
        );
    }

    private int precision = GeoTileGridAggregationBuilder.DEFAULT_PRECISION;
    private GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));

    GeoTileGridValuesSourceBuilder(String name) {
        super(name);
    }

    public GeoTileGridValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.precision = in.readInt();
        this.geoBoundingBox = new GeoBoundingBox(in);
    }

    public GeoTileGridValuesSourceBuilder precision(int precision) {
        this.precision = GeoTileUtils.checkPrecisionRange(precision);
        return this;
    }

    public GeoTileGridValuesSourceBuilder geoBoundingBox(GeoBoundingBox geoBoundingBox) {
        this.geoBoundingBox = geoBoundingBox;
        return this;
    }

    @Override
    public GeoTileGridValuesSourceBuilder format(String format) {
        throw new IllegalArgumentException("[format] is not supported for [" + TYPE + "]");
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeInt(precision);
        geoBoundingBox.writeTo(out);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("precision", precision);
        if (geoBoundingBox.isUnbounded() == false) {
            geoBoundingBox.toXContent(builder, params);
        }
    }

    @Override
    protected String type() {
        return TYPE;
    }

    GeoBoundingBox geoBoundingBox() {
        return geoBoundingBox;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, geoBoundingBox);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        GeoTileGridValuesSourceBuilder other = (GeoTileGridValuesSourceBuilder) obj;
        return Objects.equals(precision, other.precision) && Objects.equals(geoBoundingBox, other.geoBoundingBox);
    }

    @Override
    protected ValuesSourceType getDefaultValuesSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(REGISTRY_KEY, config)
            .apply(config, precision, geoBoundingBox(), name, script() != null, format(), missingBucket(), missingOrder(), order());
    }

}
