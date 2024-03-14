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
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Point;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.opensearch.index.mapper.GeoPointFieldMapper.ParsedGeoPoint;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.VectorGeoPointShapeQueryProcessor;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Field Mapper for geo_point types.
 * <p>
 * Uses lucene 6 LatLonPoint encoding
 *
 * @opensearch.internal
 */
public class GeoPointFieldMapper extends AbstractPointGeometryFieldMapper<List<ParsedGeoPoint>, List<? extends GeoPoint>> {
    public static final String CONTENT_TYPE = "geo_point";
    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }

    /**
     * Concrete builder for geo_point types
     *
     * @opensearch.internal
     */
    public static class Builder extends AbstractPointGeometryFieldMapper.Builder<Builder, GeoPointFieldType> {

        public Builder(String name) {
            super(name, FIELD_TYPE);
            hasDocValues = true;
            builder = this;
        }

        @Override
        public GeoPointFieldMapper build(
            BuilderContext context,
            String simpleName,
            FieldType fieldType,
            MultiFields multiFields,
            Explicit<Boolean> ignoreMalformed,
            Explicit<Boolean> ignoreZValue,
            ParsedPoint nullValue,
            CopyTo copyTo
        ) {
            GeoPointFieldType ft = new GeoPointFieldType(buildFullName(context), indexed, fieldType.stored(), hasDocValues, meta);
            ft.setGeometryParser(new PointParser<>(name, ParsedGeoPoint::new, (parser, point) -> {
                GeoUtils.parseGeoPoint(parser, point, ignoreZValue().value());
                return point;
            }, (ParsedGeoPoint) nullValue, ignoreZValue.value(), ignoreMalformed.value()));
            ft.setGeometryIndexer(new GeoPointIndexer(ft));
            return new GeoPointFieldMapper(name, fieldType, ft, multiFields, ignoreMalformed, ignoreZValue, nullValue, copyTo);
        }
    }

    /**
     * Concrete parser for geo_point types
     *
     * @opensearch.internal
     */
    public static class TypeParser extends AbstractPointGeometryFieldMapper.TypeParser<Builder> {
        @Override
        protected Builder newBuilder(String name, Map<String, Object> params) {
            return new GeoPointFieldMapper.Builder(name);
        }

        protected ParsedGeoPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            ParsedGeoPoint point = new ParsedGeoPoint();
            GeoUtils.parseGeoPoint(nullValue, point, ignoreZValue);
            if (ignoreMalformed == false) {
                if (point.lat() > 90.0 || point.lat() < -90.0) {
                    throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "]");
                }
                if (point.lon() > 180.0 || point.lon() < -180) {
                    throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "]");
                }
            } else {
                GeoUtils.normalizePoint(point);
            }
            return point;
        }
    }

    public GeoPointFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> ignoreZValue,
        ParsedPoint nullValue,
        CopyTo copyTo
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, ignoreMalformed, ignoreZValue, nullValue, copyTo);
    }

    @Override
    protected void addStoredFields(ParseContext context, List<? extends GeoPoint> points) {
        for (GeoPoint point : points) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
    }

    @Override
    protected void addMultiFields(ParseContext context, List<? extends GeoPoint> points) throws IOException {
        // @todo phase out geohash (which is currently used in the CompletionSuggester)
        if (points.isEmpty()) {
            return;
        }

        StringBuilder s = new StringBuilder();
        if (points.size() > 1) {
            s.append('[');
        }
        s.append(points.get(0).geohash());
        for (int i = 1; i < points.size(); ++i) {
            s.append(',');
            s.append(points.get(i).geohash());
        }
        if (points.size() > 1) {
            s.append(']');
        }
        multiFields.parse(this, context.createExternalValueContext(s));
    }

    @Override
    protected void addDocValuesFields(String name, List<? extends GeoPoint> points, List<IndexableField> fields, ParseContext context) {
        for (GeoPoint point : points) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), point.lat(), point.lon()));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public GeoPointFieldType fieldType() {
        return (GeoPointFieldType) mappedFieldType;
    }

    /**
     * Concrete field type for geo_point
     *
     * @opensearch.internal
     */
    public static class GeoPointFieldType extends AbstractPointGeometryFieldType<List<ParsedGeoPoint>, List<? extends GeoPoint>>
        implements
            GeoShapeQueryable {

        private final VectorGeoPointShapeQueryProcessor queryProcessor;

        private GeoPointFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, meta);
            this.queryProcessor = new VectorGeoPointShapeQueryProcessor();
        }

        public GeoPointFieldType(String name) {
            this(name, true, false, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            return queryProcessor.geoShapeQuery(shape, fieldName, relation, context);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new AbstractLatLonPointIndexFieldData.Builder(name(), CoreValuesSourceType.GEOPOINT);
        }

        @Override
        public Query distanceFeatureQuery(Object origin, String pivot, float boost, QueryShardContext context) {
            GeoPoint originGeoPoint;
            if (origin instanceof GeoPoint) {
                originGeoPoint = (GeoPoint) origin;
            } else if (origin instanceof String) {
                originGeoPoint = GeoUtils.parseFromString((String) origin);
            } else {
                throw new IllegalArgumentException(
                    "Illegal type ["
                        + origin.getClass()
                        + "] for [origin]! "
                        + "Must be of type [geo_point] or [string] for geo_point fields!"
                );
            }
            double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
            return LatLonPoint.newDistanceFeatureQuery(name(), boost, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        }
    }

    // Eclipse requires the AbstractPointGeometryFieldMapper prefix or it can't find ParsedPoint
    // See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565255
    /**
     * A geo point parsed from geojson
     *
     * @opensearch.internal
     */
    protected static class ParsedGeoPoint extends GeoPoint implements AbstractPointGeometryFieldMapper.ParsedPoint {
        @Override
        public void validate(String fieldName) {
            if (lat() > 90.0 || lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + lat() + "] for " + fieldName);
            }
            if (lon() > 180.0 || lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + lon() + "] for " + fieldName);
            }
        }

        @Override
        public void normalize(String name) {
            if (isNormalizable(lat()) && isNormalizable(lon())) {
                GeoUtils.normalizePoint(this);
            } else {
                throw new OpenSearchParseException("cannot normalize the point - not a number");
            }
        }

        @Override
        public boolean isNormalizable(double coord) {
            return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
        }

        @Override
        public void resetCoords(double x, double y) {
            this.reset(y, x);
        }

        public Point asGeometry() {
            return new Point(lon(), lat());
        }

        @Override
        public boolean equals(Object other) {
            double oLat;
            double oLon;
            if (other instanceof GeoPoint) {
                GeoPoint o = (GeoPoint) other;
                oLat = o.lat();
                oLon = o.lon();
            } else {
                return false;
            }
            if (Double.compare(oLat, lat) != 0) return false;
            if (Double.compare(oLon, lon) != 0) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    /**
     * The indexer for geo_point
     *
     * @opensearch.internal
     */
    protected static class GeoPointIndexer implements Indexer<List<ParsedGeoPoint>, List<? extends GeoPoint>> {

        protected final GeoPointFieldType fieldType;

        GeoPointIndexer(GeoPointFieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public List<? extends GeoPoint> prepareForIndexing(List<ParsedGeoPoint> geoPoints) {
            if (geoPoints == null || geoPoints.isEmpty()) {
                return Collections.emptyList();
            }
            return geoPoints;
        }

        @Override
        public Class<List<? extends GeoPoint>> processedClass() {
            return (Class<List<? extends GeoPoint>>) (Object) List.class;
        }

        @Override
        public List<IndexableField> indexShape(ParseContext context, List<? extends GeoPoint> points) {
            ArrayList<IndexableField> fields = new ArrayList<>(points.size());
            for (GeoPoint point : points) {
                fields.add(new LatLonPoint(fieldType.name(), point.lat(), point.lon()));
            }
            return fields;
        }
    }
}
