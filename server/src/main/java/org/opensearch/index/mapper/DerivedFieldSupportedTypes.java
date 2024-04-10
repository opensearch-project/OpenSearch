/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.opensearch.Version;
import org.opensearch.common.Booleans;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.network.InetAddresses;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Contains logic to get the FieldMapper for a given type of derived field. Also, for a given type of derived field,
 * it is used to create an IndexableField for the provided type and object. It is useful when indexing into
 * lucene MemoryIndex in {@link org.opensearch.index.query.DerivedFieldQuery}.
 */
public enum DerivedFieldSupportedTypes {

    BOOLEAN("boolean", (name, context) -> {
        BooleanFieldMapper.Builder builder = new BooleanFieldMapper.Builder(name);
        return builder.build(context);
    }, name -> o -> {
        // Trying to mimic the logic for parsing source value as used in BooleanFieldMapper valueFetcher
        Boolean value;
        if (o instanceof Boolean) {
            value = (Boolean) o;
        } else {
            String textValue = o.toString();
            value = Booleans.parseBooleanStrict(textValue, false);
        }
        return new Field(name, value ? "T" : "F", BooleanFieldMapper.Defaults.FIELD_TYPE);
    }, o -> o),
    DATE("date", (name, context) -> {
        // TODO: should we support mapping settings exposed by a given field type from derived fields too?
        // for example, support `format` for date type?
        DateFieldMapper.Builder builder = new DateFieldMapper.Builder(
            name,
            DateFieldMapper.Resolution.MILLISECONDS,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            false,
            Version.CURRENT
        );
        return builder.build(context);
    }, name -> o -> new LongPoint(name, (long) o), o -> DateFieldMapper.getDefaultDateTimeFormatter().formatMillis((long) o)),
    GEO_POINT("geo_point", (name, context) -> {
        GeoPointFieldMapper.Builder builder = new GeoPointFieldMapper.Builder(name);
        return builder.build(context);
    }, name -> o -> {
        // convert o to array of double
        if (!(o instanceof Tuple) || !(((Tuple<?, ?>) o).v1() instanceof Double || !(((Tuple<?, ?>) o).v2() instanceof Double))) {
            throw new ClassCastException("geo_point should be in format emit(double lat, double lon) for derived fields");
        }
        return new LatLonPoint(name, (double) ((Tuple<?, ?>) o).v1(), (double) ((Tuple<?, ?>) o).v2());
    }, o -> new GeoPoint((double) ((Tuple) o).v1(), (double) ((Tuple) o).v2())),
    IP("ip", (name, context) -> {
        IpFieldMapper.Builder builder = new IpFieldMapper.Builder(name, false, Version.CURRENT);
        return builder.build(context);
    }, name -> o -> {
        InetAddress address;
        if (o instanceof InetAddress) {
            address = (InetAddress) o;
        } else {
            address = InetAddresses.forString(o.toString());
        }
        return new InetAddressPoint(name, address);
    }, o -> o),
    KEYWORD("keyword", (name, context) -> {
        FieldType dummyFieldType = new FieldType();
        dummyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        KeywordFieldMapper.Builder keywordBuilder = new KeywordFieldMapper.Builder(name);
        KeywordFieldMapper.KeywordFieldType keywordFieldType = keywordBuilder.buildFieldType(context, dummyFieldType);
        keywordFieldType.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        return new KeywordFieldMapper(
            name,
            dummyFieldType,
            keywordFieldType,
            keywordBuilder.multiFieldsBuilder.build(keywordBuilder, context),
            keywordBuilder.copyTo.build(),
            keywordBuilder
        );
    }, name -> o -> new KeywordField(name, (String) o, Field.Store.NO), o -> o),
    LONG("long", (name, context) -> {
        NumberFieldMapper.Builder longBuilder = new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG, false, false);
        return longBuilder.build(context);
    }, name -> o -> new LongField(name, Long.parseLong(o.toString()), Field.Store.NO), o -> o),
    DOUBLE("double", (name, context) -> {
        NumberFieldMapper.Builder doubleBuilder = new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.DOUBLE, false, false);
        return doubleBuilder.build(context);
    }, name -> o -> new DoubleField(name, Double.parseDouble(o.toString()), Field.Store.NO), o -> o);

    final String name;
    private final BiFunction<String, Mapper.BuilderContext, FieldMapper> builder;

    private final Function<String, Function<Object, IndexableField>> indexableFieldBuilder;

    private final Function<Object, Object> valueForDisplay;

    DerivedFieldSupportedTypes(
        String name,
        BiFunction<String, Mapper.BuilderContext, FieldMapper> builder,
        Function<String, Function<Object, IndexableField>> indexableFieldBuilder,
        Function<Object, Object> valueForDisplay
    ) {
        this.name = name;
        this.builder = builder;
        this.indexableFieldBuilder = indexableFieldBuilder;
        this.valueForDisplay = valueForDisplay;
    }

    public String getName() {
        return name;
    }

    private FieldMapper getFieldMapper(String name, Mapper.BuilderContext context) {
        return builder.apply(name, context);
    }

    private Function<Object, IndexableField> getIndexableFieldGenerator(String name) {
        return indexableFieldBuilder.apply(name);
    }

    private Function<Object, Object> getValueForDisplayGenerator() {
        return valueForDisplay;
    }

    private static final Map<String, DerivedFieldSupportedTypes> enumMap = Arrays.stream(DerivedFieldSupportedTypes.values())
        .collect(Collectors.toMap(DerivedFieldSupportedTypes::getName, enumValue -> enumValue));

    public static FieldMapper getFieldMapperFromType(String type, String name, Mapper.BuilderContext context) {
        if (!enumMap.containsKey(type)) {
            throw new IllegalArgumentException("Type [" + type + "] isn't supported in Derived field context.");
        }
        return enumMap.get(type).getFieldMapper(name, context);
    }

    public static Function<Object, IndexableField> getIndexableFieldGeneratorType(String type, String name) {
        if (!enumMap.containsKey(type)) {
            throw new IllegalArgumentException("Type [" + type + "] isn't supported in Derived field context.");
        }
        return enumMap.get(type).getIndexableFieldGenerator(name);
    }

    public static Function<Object, Object> getValueForDisplayGenerator(String type) {
        if (!enumMap.containsKey(type)) {
            throw new IllegalArgumentException("Type [" + type + "] isn't supported in Derived field context.");
        }
        return enumMap.get(type).getValueForDisplayGenerator();
    }
}
