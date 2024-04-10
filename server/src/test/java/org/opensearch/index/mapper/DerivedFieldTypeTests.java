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
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.opensearch.common.collect.Tuple;
import org.opensearch.script.Script;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DerivedFieldTypeTests extends FieldTypeTestCase {

    private DerivedFieldType createDerivedFieldType(String type) {
        Mapper.BuilderContext context = mock(Mapper.BuilderContext.class);
        when(context.path()).thenReturn(new ContentPath());
        return new DerivedFieldType(
            new DerivedField(type + " _derived_field", type, new Script("")),
            DerivedFieldSupportedTypes.getFieldMapperFromType(type, type + "_derived_field", context),
            DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(type, type + "_derived_field")
        );
    }

    public void testBooleanType() {
        DerivedFieldType dft = createDerivedFieldType("boolean");
        assertTrue(dft.typeFieldMapper instanceof BooleanFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply(true) instanceof Field);
        assertTrue(dft.indexableFieldGenerator.apply(false) instanceof Field);
    }

    public void testDateType() {
        DerivedFieldType dft = createDerivedFieldType("date");
        assertTrue(dft.typeFieldMapper instanceof DateFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply(System.currentTimeMillis()) instanceof LongPoint);
        expectThrows(Exception.class, () -> dft.indexableFieldGenerator.apply("blah"));
    }

    public void testGeoPointType() {
        DerivedFieldType dft = createDerivedFieldType("geo_point");
        assertTrue(dft.typeFieldMapper instanceof GeoPointFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply(new Tuple<>(10.0, 20.0)) instanceof LatLonPoint);
        expectThrows(ClassCastException.class, () -> dft.indexableFieldGenerator.apply(List.of(10.0)));
        expectThrows(ClassCastException.class, () -> dft.indexableFieldGenerator.apply(List.of()));
        expectThrows(ClassCastException.class, () -> dft.indexableFieldGenerator.apply(List.of("10")));
        expectThrows(ClassCastException.class, () -> dft.indexableFieldGenerator.apply(List.of(10.0, 20.0, 30.0)));
    }

    public void testIPType() {
        DerivedFieldType dft = createDerivedFieldType("ip");
        assertTrue(dft.typeFieldMapper instanceof IpFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply("127.0.0.1") instanceof InetAddressPoint);
        expectThrows(Exception.class, () -> dft.indexableFieldGenerator.apply("blah"));
    }

    public void testKeywordType() {
        DerivedFieldType dft = createDerivedFieldType("keyword");
        assertTrue(dft.typeFieldMapper instanceof KeywordFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply("test_keyword") instanceof KeywordField);
        expectThrows(Exception.class, () -> dft.indexableFieldGenerator.apply(10));
    }

    public void testLongType() {
        DerivedFieldType dft = createDerivedFieldType("long");
        assertTrue(dft.typeFieldMapper instanceof NumberFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply(10) instanceof LongField);
        expectThrows(Exception.class, () -> dft.indexableFieldGenerator.apply(10.0));
    }

    public void testDoubleType() {
        DerivedFieldType dft = createDerivedFieldType("double");
        assertTrue(dft.typeFieldMapper instanceof NumberFieldMapper);
        assertTrue(dft.indexableFieldGenerator.apply(10.0) instanceof DoubleField);
        expectThrows(Exception.class, () -> dft.indexableFieldGenerator.apply(""));
    }

    public void testUnsupportedType() {
        expectThrows(IllegalArgumentException.class, () -> createDerivedFieldType("match_only_text"));
    }
}
