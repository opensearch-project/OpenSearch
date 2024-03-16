/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.painless.spi.AllowlistLoader;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.script.ScriptContext;
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
        Map<ScriptContext<?>, List<Allowlist>> contexts = newDefaultContexts();
        List<Allowlist> allowlists = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        allowlists.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.opensearch.derived.txt"));
        contexts.put(DerivedFieldScript.CONTEXT, allowlists);

        // Mocking field values to be returned


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

    // TESTCASE: Test emit function is required

    // TESTCASE: Test Long
    public void testEmittingLongField() {
        // Mocking field value to be returned

    }

    // TESTCASE: Test Double
    public void testEmittingDoubleField() throws IOException {
        // Mocking field value to be returned
        NumberFieldType fieldType = new NumberFieldType("test_double_field", NumberType.DOUBLE);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("test_double_field")).thenReturn(fieldType);

        SortedNumericDoubleValues doubleValues = mock(SortedNumericDoubleValues.class);
        when(doubleValues.advanceExact(anyInt())).thenReturn(true);
        when(doubleValues.nextValue()).thenReturn(2.718);

        LeafNumericFieldData atomicFieldData = mock(LeafNumericFieldData.class);
        when(atomicFieldData.getDoubleValues()).thenReturn(doubleValues);

        IndexNumericFieldData fieldData = mock(IndexNumericFieldData.class);
        when(fieldData.getFieldName()).thenReturn("test_double_field");
        when(fieldData.load(any())).thenReturn(atomicFieldData);

        SearchLookup lookup = new SearchLookup(mapperService, (ignored, searchLookup) -> fieldData);

        // Execute the script
        DerivedFieldScript script = compile("emitDouble(doc['test_double_field'].value)", lookup).newInstance(null);
        script.setDocument(1);

        List<Object> result = script.execute();
        assertEquals(List.of(2.718), result);
    }

    // TESTCASE: Test GeoPoint

    // TESTCASE: Test Boolean

    // TESTCASE: Test String

    // TESTCASE: Test returning multiple values
}
