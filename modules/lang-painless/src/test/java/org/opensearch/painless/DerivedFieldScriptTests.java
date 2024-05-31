/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.IndexGeoPointFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.LeafGeoPointFieldData;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.plain.AbstractLeafGeoPointFieldData;
import org.opensearch.index.fielddata.plain.LeafDoubleFieldData;
import org.opensearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.painless.spi.Whitelist;
import org.opensearch.painless.spi.WhitelistLoader;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptException;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DerivedFieldScriptTests extends ScriptTestCase {

    private static PainlessScriptEngine SCRIPT_ENGINE;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Adding derived field script to the contexts for the script engine
        Map<ScriptContext<?>, List<Whitelist>> contexts = newDefaultContexts();
        List<Whitelist> allowlists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        allowlists.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.opensearch.derived.txt"));
        contexts.put(DerivedFieldScript.CONTEXT, allowlists);

        SCRIPT_ENGINE = new PainlessScriptEngine(Settings.EMPTY, contexts);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        SCRIPT_ENGINE = null;
    }

    @Override
    protected PainlessScriptEngine getEngine() {
        return SCRIPT_ENGINE;
    }

    private DerivedFieldScript.LeafFactory compile(String expression, SearchLookup lookup) {
        DerivedFieldScript.Factory factory = SCRIPT_ENGINE.compile(
            "derived_script_test",
            expression,
            DerivedFieldScript.CONTEXT,
            Collections.emptyMap()
        );
        return factory.newFactory(Collections.emptyMap(), lookup);
    }

    public void testEmittingDoubleField() throws IOException {
        // Mocking field value to be returned
        NumberFieldType fieldType = new NumberFieldType("test_double_field", NumberType.DOUBLE);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("test_double_field")).thenReturn(fieldType);

        SortedNumericDoubleValues doubleValues = mock(SortedNumericDoubleValues.class);
        when(doubleValues.docValueCount()).thenReturn(1);
        when(doubleValues.advanceExact(anyInt())).thenReturn(true);
        when(doubleValues.nextValue()).thenReturn(2.718);

        LeafNumericFieldData atomicFieldData = mock(LeafDoubleFieldData.class); // SortedNumericDoubleFieldData
        when(atomicFieldData.getDoubleValues()).thenReturn(doubleValues);

        IndexNumericFieldData fieldData = mock(IndexNumericFieldData.class); // SortedNumericIndexFieldData
        when(fieldData.getFieldName()).thenReturn("test_double_field");
        when(fieldData.load(any())).thenReturn(atomicFieldData);

        SearchLookup lookup = new SearchLookup(mapperService, (ignored, searchLookup) -> fieldData);

        // We don't need a real index, just need to construct a LeafReaderContext which cannot be mocked
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

        // Execute the script
        DerivedFieldScript script = compile("emit(doc['test_double_field'].value)", lookup).newInstance(leafReaderContext);
        script.setDocument(1);
        script.execute();

        List<Object> result = script.getEmittedValues();
        assertEquals(List.of(2.718), result);
    }

    public void testEmittingGeoPoint() throws IOException {
        // Mocking field value to be returned
        GeoPointFieldType fieldType = new GeoPointFieldType("test_geo_field");
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("test_geo_field")).thenReturn(fieldType);

        MultiGeoPointValues geoPointValues = mock(MultiGeoPointValues.class);
        when(geoPointValues.docValueCount()).thenReturn(1);
        when(geoPointValues.advanceExact(anyInt())).thenReturn(true);
        when(geoPointValues.nextValue()).thenReturn(new GeoPoint(5, 8));

        LeafGeoPointFieldData atomicFieldData = mock(AbstractLeafGeoPointFieldData.class); // LatLonPointDVLeafFieldData
        when(atomicFieldData.getGeoPointValues()).thenReturn(geoPointValues);

        IndexGeoPointFieldData fieldData = mock(IndexGeoPointFieldData.class);
        when(fieldData.getFieldName()).thenReturn("test_geo_field");
        when(fieldData.load(any())).thenReturn(atomicFieldData);

        SearchLookup lookup = new SearchLookup(mapperService, (ignored, searchLookup) -> fieldData);

        // We don't need a real index, just need to construct a LeafReaderContext which cannot be mocked
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

        // Execute the script
        DerivedFieldScript script = compile("emit(doc['test_geo_field'].value.getLat(), doc['test_geo_field'].value.getLon())", lookup)
            .newInstance(leafReaderContext);
        script.setDocument(1);
        script.execute();

        List<Object> result = script.getEmittedValues();
        assertEquals(List.of(new Tuple<>(5.0, 8.0)), result);
    }

    public void testEmittingMultipleValues() throws IOException {
        SearchLookup lookup = mock(SearchLookup.class);

        // We don't need a real index, just need to construct a LeafReaderContext which cannot be mocked
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

        LeafSearchLookup leafSearchLookup = mock(LeafSearchLookup.class);
        when(lookup.getLeafSearchLookup(leafReaderContext)).thenReturn(leafSearchLookup);

        // Execute the script
        DerivedFieldScript script = compile(
            "def l = new ArrayList(); l.add('test'); l.add('multiple'); l.add('values'); for (String x : l) emit(x)",
            lookup
        ).newInstance(leafReaderContext);
        script.setDocument(1);
        script.execute();

        List<Object> result = script.getEmittedValues();
        assertEquals(List.of("test", "multiple", "values"), result);
    }

    public void testExceedingByteSizeLimit() throws IOException {
        SearchLookup lookup = mock(SearchLookup.class);

        // We don't need a real index, just need to construct a LeafReaderContext which cannot be mocked
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

        LeafSearchLookup leafSearchLookup = mock(LeafSearchLookup.class);
        when(lookup.getLeafSearchLookup(leafReaderContext)).thenReturn(leafSearchLookup);

        // Emitting a large string to exceed the byte size limit
        DerivedFieldScript stringScript = compile("for (int i = 0; i < 1024 * 1024; i++) emit('a' + i);", lookup).newInstance(
            leafReaderContext
        );
        expectThrows(ScriptException.class, () -> {
            stringScript.setDocument(1);
            stringScript.execute();
        });

        // Emitting an integer to check byte size limit
        DerivedFieldScript intScript = compile("for (int i = 0; i < 1024 * 1024; i++) emit(42)", lookup).newInstance(leafReaderContext);
        expectThrows(ScriptException.class, "Expected IllegalStateException for exceeding byte size limit", () -> {
            intScript.setDocument(1);
            intScript.execute();
        });

        // Emitting a long to check byte size limit
        DerivedFieldScript longScript = compile("for (int i = 0; i < 1024 * 1024; i++) emit(1234567890123456789L)", lookup).newInstance(
            leafReaderContext
        );
        expectThrows(ScriptException.class, "Expected IllegalStateException for exceeding byte size limit", () -> {
            longScript.setDocument(1);
            longScript.execute();
        });

        // Emitting a double to check byte size limit
        DerivedFieldScript doubleScript = compile("for (int i = 0; i < 1024 * 1024; i++) emit(3.14159)", lookup).newInstance(
            leafReaderContext
        );
        expectThrows(ScriptException.class, "Expected IllegalStateException for exceeding byte size limit", () -> {
            doubleScript.setDocument(1);
            doubleScript.execute();
        });

        // Emitting a GeoPoint to check byte size limit
        DerivedFieldScript geoPointScript = compile("for (int i = 0; i < 1024 * 1024; i++) emit(1.23, 4.56);", lookup).newInstance(
            leafReaderContext
        );
        expectThrows(ScriptException.class, "Expected IllegalStateException for exceeding byte size limit", () -> {
            geoPointScript.setDocument(1);
            geoPointScript.execute();
        });
    }
}
