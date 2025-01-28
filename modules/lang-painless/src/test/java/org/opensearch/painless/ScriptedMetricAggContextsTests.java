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

package org.opensearch.painless;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.settings.Settings;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptedMetricAggContexts;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptedMetricAggContextsTests extends ScriptTestCase {
    private static PainlessScriptEngine SCRIPT_ENGINE;

    @BeforeClass
    public static void beforeClass() {
        Map<ScriptContext<?>, List<Allowlist>> contexts = new HashMap<>();
        contexts.put(ScriptedMetricAggContexts.InitScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        contexts.put(ScriptedMetricAggContexts.MapScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        contexts.put(ScriptedMetricAggContexts.CombineScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        contexts.put(ScriptedMetricAggContexts.ReduceScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        SCRIPT_ENGINE = new PainlessScriptEngine(Settings.EMPTY, contexts);
    }

    @AfterClass
    public static void afterClass() {
        SCRIPT_ENGINE = null;
    }

    @Override
    protected PainlessScriptEngine getEngine() {
        return SCRIPT_ENGINE;
    }

    public void testInitBasic() {
        ScriptedMetricAggContexts.InitScript.Factory factory = getEngine().compile(
            "test",
            "state.testField = params.initialVal",
            ScriptedMetricAggContexts.InitScript.CONTEXT,
            Collections.emptyMap()
        );

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        params.put("initialVal", 10);

        ScriptedMetricAggContexts.InitScript script = factory.newInstance(params, state);
        script.execute();

        assert (state.containsKey("testField"));
        assertEquals(10, state.get("testField"));
    }

    public void testMapBasic() throws IOException {
        ScriptedMetricAggContexts.MapScript.Factory factory = getEngine().compile(
            "test",
            "state.testField = 2*_score",
            ScriptedMetricAggContexts.MapScript.CONTEXT,
            Collections.emptyMap()
        );

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        Scorable scorer = new Scorable() {
            @Override
            public float score() {
                return 0.5f;
            }
        };

        ScriptedMetricAggContexts.MapScript.LeafFactory leafFactory = factory.newFactory(params, state, null);
        ScriptedMetricAggContexts.MapScript script = leafFactory.newInstance(null);

        script.setScorer(scorer);
        script.execute();

        assert (state.containsKey("testField"));
        assertEquals(1.0, state.get("testField"));
    }

    public void testReturnSource() throws IOException {
        ScriptedMetricAggContexts.MapScript.Factory factory = getEngine().compile(
            "test",
            "state._source = params._source",
            ScriptedMetricAggContexts.MapScript.CONTEXT,
            Collections.emptyMap()
        );

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        MemoryIndex index = new MemoryIndex();
        // we don't need a real index, just need to construct a LeafReaderContext which cannot be mocked
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

        SearchLookup lookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(lookup.getLeafSearchLookup(leafReaderContext)).thenReturn(leafLookup);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(leafLookup.asMap()).thenReturn(Collections.singletonMap("_source", sourceLookup));
        when(sourceLookup.loadSourceIfNeeded()).thenReturn(Collections.singletonMap("test", 1));
        ScriptedMetricAggContexts.MapScript.LeafFactory leafFactory = factory.newFactory(params, state, lookup);
        ScriptedMetricAggContexts.MapScript script = leafFactory.newInstance(leafReaderContext);

        script.execute();

        assertTrue(state.containsKey("_source"));
        assertTrue(state.get("_source") instanceof Map && ((Map) state.get("_source")).containsKey("test"));
        assertEquals(1, ((Map) state.get("_source")).get("test"));
    }

    public void testMapSourceAccess() throws IOException {
        ScriptedMetricAggContexts.MapScript.Factory factory = getEngine().compile(
            "test",
            "state.testField = params._source.three",
            ScriptedMetricAggContexts.MapScript.CONTEXT,
            Collections.emptyMap()
        );

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        MemoryIndex index = new MemoryIndex();
        // we don't need a real index, just need to construct a LeafReaderContext which cannot be mocked
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

        SearchLookup lookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(lookup.getLeafSearchLookup(leafReaderContext)).thenReturn(leafLookup);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(leafLookup.asMap()).thenReturn(Collections.singletonMap("_source", sourceLookup));
        when(sourceLookup.loadSourceIfNeeded()).thenReturn(Collections.singletonMap("three", 3));
        ScriptedMetricAggContexts.MapScript.LeafFactory leafFactory = factory.newFactory(params, state, lookup);
        ScriptedMetricAggContexts.MapScript script = leafFactory.newInstance(leafReaderContext);

        script.execute();

        assertTrue(state.containsKey("testField"));
        assertEquals(3, state.get("testField"));
    }

    public void testCombineBasic() {
        ScriptedMetricAggContexts.CombineScript.Factory factory = getEngine().compile(
            "test",
            "state.testField = params.initialVal; return state.testField + params.inc",
            ScriptedMetricAggContexts.CombineScript.CONTEXT,
            Collections.emptyMap()
        );

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        params.put("initialVal", 10);
        params.put("inc", 2);

        ScriptedMetricAggContexts.CombineScript script = factory.newInstance(params, state);
        Object res = script.execute();

        assert (state.containsKey("testField"));
        assertEquals(10, state.get("testField"));
        assertEquals(12, res);
    }

    public void testReduceBasic() {
        ScriptedMetricAggContexts.ReduceScript.Factory factory = getEngine().compile(
            "test",
            "states[0].testField + states[1].testField",
            ScriptedMetricAggContexts.ReduceScript.CONTEXT,
            Collections.emptyMap()
        );

        Map<String, Object> params = new HashMap<>();
        List<Object> states = new ArrayList<>();

        Map<String, Object> state1 = new HashMap<>(), state2 = new HashMap<>();
        state1.put("testField", 1);
        state2.put("testField", 2);

        states.add(state1);
        states.add(state2);

        ScriptedMetricAggContexts.ReduceScript script = factory.newInstance(params, states);
        Object res = script.execute();
        assertEquals(3, res);
    }
}
