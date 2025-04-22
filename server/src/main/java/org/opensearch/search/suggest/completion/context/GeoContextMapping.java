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

package org.opensearch.search.suggest.completion.context;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParseContext.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.geometry.utils.Geohash.addNeighbors;
import static org.opensearch.geometry.utils.Geohash.stringEncode;

/**
 * A {@link ContextMapping} that uses a geo location/area as a
 * criteria.
 * The suggestions can be boosted and/or filtered depending on
 * whether it falls within an area, represented by a query geo hash
 * with a specified precision
 * <p>
 * {@link GeoQueryContext} defines the options for constructing
 * a unit of query context for this context type
 *
 * @opensearch.internal
 */
public class GeoContextMapping extends ContextMapping<GeoQueryContext> {

    public static final String FIELD_PRECISION = "precision";
    public static final String FIELD_FIELDNAME = "path";

    public static final int DEFAULT_PRECISION = 6;

    static final String CONTEXT_VALUE = "context";
    static final String CONTEXT_BOOST = "boost";
    static final String CONTEXT_PRECISION = "precision";
    static final String CONTEXT_NEIGHBOURS = "neighbours";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GeoContextMapping.class);

    private final int precision;
    private final String fieldName;

    private GeoContextMapping(String name, String fieldName, int precision) {
        super(Type.GEO, name);
        this.precision = precision;
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getPrecision() {
        return precision;
    }

    protected static GeoContextMapping load(String name, Map<String, Object> config) {
        final GeoContextMapping.Builder builder = new GeoContextMapping.Builder(name);

        if (config != null) {
            final Object configPrecision = config.get(FIELD_PRECISION);
            if (configPrecision != null) {
                if (configPrecision instanceof Integer) {
                    builder.precision((Integer) configPrecision);
                } else if (configPrecision instanceof Long) {
                    builder.precision((Long) configPrecision);
                } else if (configPrecision instanceof Double) {
                    builder.precision((Double) configPrecision);
                } else if (configPrecision instanceof Float) {
                    builder.precision((Float) configPrecision);
                } else {
                    builder.precision(configPrecision.toString());
                }
                config.remove(FIELD_PRECISION);
            }

            final Object fieldName = config.get(FIELD_FIELDNAME);
            if (fieldName != null) {
                builder.field(fieldName.toString());
                config.remove(FIELD_FIELDNAME);
            }
        }
        return builder.build();
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_PRECISION, precision);
        if (fieldName != null) {
            builder.field(FIELD_FIELDNAME, fieldName);
        }
        return builder;
    }

    /**
     * Parse a set of {@link CharSequence} contexts at index-time.
     * Acceptable formats:
     *
     *  <ul>
     *     <li>Array: <pre>[<i>&lt;GEO POINT&gt;</i>, ..]</pre></li>
     *     <li>String/Object/Array: <pre>&quot;GEO POINT&quot;</pre></li>
     *  </ul>
     *
     * see {@code GeoPoint(String)} for GEO POINT
     */
    @Override
    public Set<String> parseContext(ParseContext parseContext, XContentParser parser) throws IOException, OpenSearchParseException {
        if (fieldName != null) {
            MappedFieldType fieldType = parseContext.mapperService().fieldType(fieldName);
            if (!(fieldType != null && fieldType.unwrap() instanceof GeoPointFieldMapper.GeoPointFieldType)) {
                throw new OpenSearchParseException("referenced field must be mapped to geo_point");
            }
        }
        final Set<String> contexts = new HashSet<>();
        Token token = parser.currentToken();
        if (token == Token.START_ARRAY) {
            token = parser.nextToken();
            // Test if value is a single point in <code>[lon, lat]</code> format
            if (token == Token.VALUE_NUMBER) {
                double lon = parser.doubleValue();
                if (parser.nextToken() == Token.VALUE_NUMBER) {
                    double lat = parser.doubleValue();
                    if (parser.nextToken() == Token.END_ARRAY) {
                        contexts.add(stringEncode(lon, lat, precision));
                    } else {
                        throw new OpenSearchParseException("only two values [lon, lat] expected");
                    }
                } else {
                    throw new OpenSearchParseException("latitude must be a numeric value");
                }
            } else {
                while (token != Token.END_ARRAY) {
                    GeoPoint point = GeoUtils.parseGeoPoint(parser);
                    contexts.add(stringEncode(point.getLon(), point.getLat(), precision));
                    token = parser.nextToken();
                }
            }
        } else if (token == Token.VALUE_STRING) {
            final String geoHash = parser.text();
            final CharSequence truncatedGeoHash = geoHash.subSequence(0, Math.min(geoHash.length(), precision));
            contexts.add(truncatedGeoHash.toString());
        } else {
            // or a single location
            GeoPoint point = GeoUtils.parseGeoPoint(parser);
            contexts.add(stringEncode(point.getLon(), point.getLat(), precision));
        }
        return contexts;
    }

    @Override
    public Set<String> parseContext(Document document) {
        final Set<String> geohashes = new HashSet<>();

        if (fieldName != null) {
            IndexableField[] fields = document.getFields(fieldName);
            GeoPoint spare = new GeoPoint();
            if (fields.length == 0) {
                IndexableField[] lonFields = document.getFields(fieldName + ".lon");
                IndexableField[] latFields = document.getFields(fieldName + ".lat");
                if (lonFields.length > 0 && latFields.length > 0) {
                    for (int i = 0; i < lonFields.length; i++) {
                        IndexableField lonField = lonFields[i];
                        IndexableField latField = latFields[i];
                        assert lonField.fieldType().docValuesType() == latField.fieldType().docValuesType();
                        // we write doc values fields differently: one field for all values, so we need to only care about indexed fields
                        if (lonField.fieldType().docValuesType() == DocValuesType.NONE) {
                            spare.reset(latField.numericValue().doubleValue(), lonField.numericValue().doubleValue());
                            geohashes.add(stringEncode(spare.getLon(), spare.getLat(), precision));
                        }
                    }
                }
            } else {
                for (IndexableField field : fields) {
                    if (field instanceof StringField) {
                        spare.resetFromString(field.stringValue());
                        geohashes.add(spare.geohash());
                    } else if (field instanceof LatLonPoint || field instanceof LatLonDocValuesField) {
                        spare.resetFromIndexableField(field);
                        geohashes.add(spare.geohash());
                    }
                }
            }
        }

        Set<String> locations = new HashSet<>();
        for (String geohash : geohashes) {
            int precision = Math.min(this.precision, geohash.length());
            String truncatedGeohash = geohash.substring(0, precision);
            locations.add(truncatedGeohash);
        }
        return locations;
    }

    @Override
    protected GeoQueryContext fromXContent(XContentParser parser) throws IOException {
        return GeoQueryContext.fromXContent(parser);
    }

    /**
     * Parse a list of {@link GeoQueryContext}
     * using <code>parser</code>. A QueryContexts accepts one of the following forms:
     *
     * <ul>
     *     <li>Object: GeoQueryContext</li>
     *     <li>String: GeoQueryContext value with boost=1  precision=PRECISION neighbours=[PRECISION]</li>
     *     <li>Array: <pre>[GeoQueryContext, ..]</pre></li>
     * </ul>
     *
     *  A GeoQueryContext has one of the following forms:
     *  <ul>
     *     <li>Object:
     *     <ul>
     *         <li><pre>GEO POINT</pre></li>
     *         <li><pre>{&quot;lat&quot;: <i>&lt;double&gt;</i>, &quot;lon&quot;: <i>&lt;double&gt;</i>, &quot;precision&quot;:
     *         <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *         <li><pre>{&quot;context&quot;: <i>&lt;string&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;precision&quot;:
     *         <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *         <li><pre>{&quot;context&quot;: <i>&lt;GEO POINT&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;precision&quot;:
     *         <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *     </ul>
     *     <li>String: <pre>GEO POINT</pre></li>
     *  </ul>
     * see {@code GeoPoint(String)} for GEO POINT
     */
    @Override
    public List<InternalQueryContext> toInternalQueryContexts(List<GeoQueryContext> queryContexts) {
        List<InternalQueryContext> internalQueryContextList = new ArrayList<>();
        for (GeoQueryContext queryContext : queryContexts) {
            int minPrecision = Math.min(this.precision, queryContext.getPrecision());
            GeoPoint point = queryContext.getGeoPoint();
            final Collection<String> locations = new HashSet<>();
            String geoHash = stringEncode(point.getLon(), point.getLat(), minPrecision);
            locations.add(geoHash);
            if (queryContext.getNeighbours().isEmpty() && geoHash.length() == this.precision) {
                addNeighbors(geoHash, locations);
            } else if (queryContext.getNeighbours().isEmpty() == false) {
                queryContext.getNeighbours()
                    .stream()
                    .filter(neighbourPrecision -> neighbourPrecision < geoHash.length())
                    .forEach(neighbourPrecision -> {
                        String truncatedGeoHash = geoHash.substring(0, neighbourPrecision);
                        locations.add(truncatedGeoHash);
                        addNeighbors(truncatedGeoHash, locations);
                    });
            }
            internalQueryContextList.addAll(
                locations.stream()
                    .map(location -> new InternalQueryContext(location, queryContext.getBoost(), location.length() < this.precision))
                    .collect(Collectors.toList())
            );
        }
        return internalQueryContextList;
    }

    @Override
    public void validateReferences(Version indexVersionCreated, Function<String, MappedFieldType> fieldResolver) {
        if (fieldName != null) {
            MappedFieldType mappedFieldType = fieldResolver.apply(fieldName);
            if (mappedFieldType == null) {
                throw new OpenSearchParseException("field [{}] referenced in context [{}] is not defined in the mapping", fieldName, name);
            } else if (GeoPointFieldMapper.CONTENT_TYPE.equals(mappedFieldType.typeName()) == false) {
                throw new OpenSearchParseException(
                    "field [{}] referenced in context [{}] must be mapped to geo_point, found [{}]",
                    fieldName,
                    name,
                    mappedFieldType.typeName()
                );
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeoContextMapping that = (GeoContextMapping) o;
        if (precision != that.precision) return false;
        return !(fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null);

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, fieldName);
    }

    /**
     * Builder for geo context mapping
     *
     * @opensearch.internal
     */
    public static class Builder extends ContextBuilder<GeoContextMapping> {

        private int precision = DEFAULT_PRECISION;
        private String fieldName = null;

        public Builder(String name) {
            super(name);
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param precision
         *            precision as distance with {@link DistanceUnit}. Default:
         *            meters
         * @return this
         */
        public Builder precision(String precision) {
            return precision(DistanceUnit.parse(precision, DistanceUnit.METERS, DistanceUnit.METERS));
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param precision
         *            precision value
         * @param unit
         *            {@link DistanceUnit} to use
         * @return this
         */
        public Builder precision(double precision, DistanceUnit unit) {
            return precision(unit.toMeters(precision));
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param meters
         *            precision as distance in meters
         * @return this
         */
        public Builder precision(double meters) {
            int level = GeoUtils.geoHashLevelsForPrecision(meters);
            // Ceiling precision: we might return more results
            if (GeoUtils.geoHashCellSize(level) < meters) {
                level = Math.max(1, level - 1);
            }
            return precision(level);
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param level
         *            maximum length of geohashes
         * @return this
         */
        public Builder precision(int level) {
            this.precision = level;
            return this;
        }

        /**
         * Set the name of the field containing a geolocation to use
         * @param fieldName name of the field
         * @return this
         */
        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public GeoContextMapping build() {
            return new GeoContextMapping(name, fieldName, precision);
        }
    }
}
