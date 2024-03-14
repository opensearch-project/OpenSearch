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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.GeometryParser;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.AbstractGeoShapeIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.VectorGeoShapeQueryProcessor;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.opensearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 *
 * @opensearch.internal
 */
public class GeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry, Geometry> {
    private static final Logger logger = LogManager.getLogger(GeoShapeFieldMapper.class);
    public static final String CONTENT_TYPE = "geo_shape";
    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setDimensions(7, 4, Integer.BYTES);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    /**
     * Concrete builder for geo_shape types
     *
     * @opensearch.internal
     */
    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<Builder, GeoShapeFieldType> {

        public Builder(String name) {
            super(name, FIELD_TYPE);
            this.hasDocValues = true;
            builder = this;
        }

        private GeoShapeFieldType buildFieldType(BuilderContext context) {
            GeoShapeFieldType ft = new GeoShapeFieldType(buildFullName(context), indexed, fieldType.stored(), hasDocValues, meta);
            GeometryParser geometryParser = new GeometryParser(ft.orientation.getAsBoolean(), coerce().value(), ignoreZValue().value());
            ft.setGeometryParser(new GeoShapeParser(geometryParser));
            ft.setGeometryIndexer(new GeoShapeIndexer(orientation().value().getAsBoolean(), buildFullName(context)));
            ft.setOrientation(orientation == null ? Defaults.ORIENTATION.value() : orientation);
            return ft;
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            return new GeoShapeFieldMapper(
                name,
                fieldType,
                buildFieldType(context),
                ignoreMalformed(context),
                coerce(context),
                ignoreZValue(),
                orientation(),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

    /**
     * Concrete field type for geo_shape fields
     *
     * @opensearch.internal
     */
    public static class GeoShapeFieldType extends AbstractShapeGeometryFieldType<Geometry, Geometry> implements GeoShapeQueryable {

        private final VectorGeoShapeQueryProcessor queryProcessor;

        public GeoShapeFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, false, meta);
            this.queryProcessor = new VectorGeoShapeQueryProcessor();
        }

        public GeoShapeFieldType(String name) {
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
            return new AbstractGeoShapeIndexFieldData.Builder(name(), CoreValuesSourceType.GEO_SHAPE);
        }
    }

    /**
     * The type parser
     *
     * @opensearch.internal
     */
    public static final class TypeParser extends AbstractShapeGeometryFieldMapper.TypeParser {

        @Override
        protected AbstractShapeGeometryFieldMapper.Builder newBuilder(String name, Map<String, Object> params) {
            if (params.containsKey(DEPRECATED_PARAMETERS_KEY)) {
                return new LegacyGeoShapeFieldMapper.Builder(
                    name,
                    (LegacyGeoShapeFieldMapper.DeprecatedParameters) params.get(DEPRECATED_PARAMETERS_KEY)
                );
            }
            return new GeoShapeFieldMapper.Builder(name);
        }
    }

    public GeoShapeFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> coerce,
        Explicit<Boolean> ignoreZValue,
        Explicit<ShapeBuilder.Orientation> orientation,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation, multiFields, copyTo);
    }

    @Override
    protected void addStoredFields(ParseContext context, Geometry geometry) {
        // noop: we currently do not store geo_shapes
        // @todo store as geojson string?
    }

    @Override
    protected void addDocValuesFields(
        final String name,
        final Geometry geometry,
        final List<IndexableField> indexableFields,
        final ParseContext context
    ) {
        /*
         * We are adding the doc values for GeoShape only if the index is created with 2.9 and above version of
         * OpenSearch. If we don't do that after the upgrade of OpenSearch customers are not able to index documents
         * with GeoShape fields. Github issue: https://github.com/opensearch-project/OpenSearch/issues/10958,
         * https://github.com/opensearch-project/OpenSearch/issues/10795
         */
        if (context.indexSettings().getIndexVersionCreated().onOrAfter(Version.V_2_9_0)) {
            Field[] fieldsArray = new Field[indexableFields.size()];
            fieldsArray = indexableFields.toArray(fieldsArray);
            context.doc().add(LatLonShape.createDocValueField(name, fieldsArray));
        } else {
            logger.warn(
                "The index was created with Version : {}, for geoshape doc values to work index must be "
                    + "created with OpenSearch Version : {} or above",
                context.indexSettings().getIndexVersionCreated(),
                Version.V_2_9_0
            );
        }
    }

    @Override
    protected void addMultiFields(ParseContext context, Geometry geometry) {
        // noop (completion suggester currently not compatible with geo_shape)
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper<?, ?> mergeWith, List<String> conflicts) {
        if (mergeWith instanceof LegacyGeoShapeFieldMapper) {
            LegacyGeoShapeFieldMapper legacy = (LegacyGeoShapeFieldMapper) mergeWith;
            throw new IllegalArgumentException(
                "["
                    + fieldType().name()
                    + "] with field mapper ["
                    + fieldType().typeName()
                    + "] "
                    + "using [BKD] strategy cannot be merged with "
                    + "["
                    + legacy.fieldType().typeName()
                    + "] with ["
                    + legacy.fieldType().strategy()
                    + "] strategy"
            );
        }
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
