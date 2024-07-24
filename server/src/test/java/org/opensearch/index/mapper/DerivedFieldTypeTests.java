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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.AggregationScript;
import org.opensearch.script.Script;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.index.IndexOptions.NONE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DerivedFieldTypeTests extends FieldTypeTestCase {

    private DerivedFieldType createDerivedFieldType(String type) {
        Mapper.BuilderContext context = mock(Mapper.BuilderContext.class);
        when(context.path()).thenReturn(new ContentPath());
        return new DerivedFieldType(
            new DerivedField(type + " _derived_field", type, new Script("")),
            DerivedFieldSupportedTypes.getFieldMapperFromType(type, type + "_derived_field", context, null),
            DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(type, type + "_derived_field"),
            null
        );
    }

    public void testBooleanType() {
        DerivedFieldType dft = createDerivedFieldType("boolean");
        assertTrue(dft.getFieldMapper() instanceof BooleanFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply(true) instanceof Field);
        assertTrue(dft.getIndexableFieldGenerator().apply(false) instanceof Field);
    }

    public void testDateType() {
        DerivedFieldType dft = createDerivedFieldType("date");
        assertTrue(dft.getFieldMapper() instanceof DateFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply(System.currentTimeMillis()) instanceof LongPoint);
        expectThrows(Exception.class, () -> dft.getIndexableFieldGenerator().apply("blah"));
    }

    public void testGeoPointType() {
        DerivedFieldType dft = createDerivedFieldType("geo_point");
        assertTrue(dft.getFieldMapper() instanceof GeoPointFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply(new Tuple<>(10.0, 20.0)) instanceof LatLonPoint);
        expectThrows(ClassCastException.class, () -> dft.getIndexableFieldGenerator().apply(List.of(10.0)));
        expectThrows(ClassCastException.class, () -> dft.getIndexableFieldGenerator().apply(List.of()));
        expectThrows(ClassCastException.class, () -> dft.getIndexableFieldGenerator().apply(List.of("10")));
        expectThrows(ClassCastException.class, () -> dft.getIndexableFieldGenerator().apply(List.of(10.0, 20.0, 30.0)));
    }

    public void testIPType() {
        DerivedFieldType dft = createDerivedFieldType("ip");
        assertTrue(dft.getFieldMapper() instanceof IpFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply("127.0.0.1") instanceof InetAddressPoint);
        expectThrows(Exception.class, () -> dft.getIndexableFieldGenerator().apply("blah"));
    }

    public void testKeywordType() {
        DerivedFieldType dft = createDerivedFieldType("keyword");
        assertTrue(dft.getFieldMapper() instanceof KeywordFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply("test_keyword") instanceof KeywordField);
        expectThrows(Exception.class, () -> dft.getIndexableFieldGenerator().apply(10));
    }

    public void testLongType() {
        DerivedFieldType dft = createDerivedFieldType("long");
        assertTrue(dft.getFieldMapper() instanceof NumberFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply(10) instanceof LongField);
        expectThrows(Exception.class, () -> dft.getIndexableFieldGenerator().apply(10.0));
    }

    public void testDoubleType() {
        DerivedFieldType dft = createDerivedFieldType("double");
        assertTrue(dft.getFieldMapper() instanceof NumberFieldMapper);
        assertTrue(dft.getIndexableFieldGenerator().apply(10.0) instanceof DoubleField);
        expectThrows(Exception.class, () -> dft.getIndexableFieldGenerator().apply(""));
    }

    public void testObjectType() {
        DerivedFieldType dft = createDerivedFieldType("object");
        assertTrue(dft.getFieldMapper() instanceof KeywordFieldMapper);
        assertEquals(dft.getFieldMapper().fieldType.indexOptions(), NONE);
        assertThrows(OpenSearchException.class, () -> dft.getIndexableFieldGenerator().apply(""));
    }

    public void testUnsupportedType() {
        expectThrows(IllegalArgumentException.class, () -> createDerivedFieldType("match_only_text"));
    }

    public void testGetAggregationScript_keyword() throws IOException {
        DerivedFieldType dft = spy(createDerivedFieldType("keyword"));
        QueryShardContext mockContext = mock(QueryShardContext.class);
        List<Object> expected = List.of("foo");
        mockValueFetcherForAggs(mockContext, dft, expected);

        AggregationScript.LeafFactory aggregationScript = dft.getAggregationScript(mockContext);
        // have to use a memoryIndex because we can't mock leafReaderContext
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);
        AggregationScript script = aggregationScript.newInstance(leafReaderContext);

        Object result = script.execute();
        assertEquals(expected, result);
    }

    public void testGetAggregationScript_ip() throws IOException {
        DerivedFieldType dft = spy(createDerivedFieldType("ip"));
        QueryShardContext mockContext = mock(QueryShardContext.class);
        List<Object> expected = List.of("192.168.0.1");
        LeafSearchLookup leafSearchLookup = mockValueFetcherForAggs(mockContext, dft, expected);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(leafSearchLookup.source()).thenReturn(sourceLookup);
        AggregationScript.LeafFactory aggregationScript = dft.getAggregationScript(mockContext);
        assertFalse(aggregationScript.needs_score());
        // have to use a memoryIndex because we can't mock leafReaderContext
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);
        AggregationScript script = aggregationScript.newInstance(leafReaderContext);

        // test setDocument
        int docid = 1;
        script.setDocument(docid);
        verify(sourceLookup, times(1)).setSegmentAndDocument(any(), eq(docid));

        // test execute
        List<Object> result = (List<Object>) script.execute();
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString((String) expected.get(0)))), result.get(0));
    }

    private static LeafSearchLookup mockValueFetcherForAggs(QueryShardContext mockContext, DerivedFieldType dft, List<Object> expected) {
        SearchLookup searchLookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(searchLookup.getLeafSearchLookup(any())).thenReturn(leafLookup);
        when(mockContext.lookup()).thenReturn(searchLookup);
        DerivedFieldValueFetcher valueFetcher = mock(DerivedFieldValueFetcher.class);
        when(valueFetcher.fetchValuesInternal(any())).thenReturn(expected);
        doReturn(valueFetcher).when(dft).valueFetcher(any(), any(), any());
        return leafLookup;
    }
}
