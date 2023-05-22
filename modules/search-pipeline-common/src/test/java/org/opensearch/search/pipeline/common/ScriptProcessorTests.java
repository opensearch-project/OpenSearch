/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.junit.Before;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.SearchScript;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.common.helpers.SearchRequestMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import static org.hamcrest.core.Is.is;
import java.util.concurrent.TimeUnit;

public class ScriptProcessorTests extends OpenSearchTestCase {

    private ScriptService scriptService;
    private Script script;
    private SearchScript searchScript;

    @Before
    public void setupScripting() {
        String scriptName = "search_script";
        scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> {
                Object sourceObj = ctx.get("source");
                if (sourceObj instanceof Map) {
                    Map<String, Object> source = (SearchRequestMap) sourceObj;

                    // Update all modifiable source fields
                    Integer from = (Integer) source.get("from");
                    source.put("from", from + 10);

                    Integer size = (Integer) source.get("size");
                    source.put("size", size + 10);

                    Boolean explain = (Boolean) source.get("explain");
                    source.put("explain", !explain);

                    Boolean version = (Boolean) source.get("version");
                    source.put("version", !version);

                    Boolean seqNoAndPrimaryTerm = (Boolean) source.get("seqNoAndPrimaryTerm");
                    source.put("seqNoAndPrimaryTerm", !seqNoAndPrimaryTerm);

                    Boolean trackScores = (Boolean) source.get("trackScores");
                    source.put("trackScores", !trackScores);

                    Integer trackTotalHitsUpTo = (Integer) source.get("trackTotalHitsUpTo");
                    source.put("trackTotalHitsUpTo", trackTotalHitsUpTo + 1);

                    Float minScore = (Float) source.get("minScore");
                    source.put("minScore", minScore + 1.0f);

                    Integer terminateAfter = (Integer) source.get("terminateAfter");
                    source.put("terminateAfter", terminateAfter + 1);
                }
                return null;
            }), Collections.emptyMap())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap());
        searchScript = scriptService.compile(script, SearchScript.CONTEXT).newInstance(script.getParams());
    }

    public void testScriptingWithoutPrecompiledScriptFactory() throws Exception {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(createSearchSourceBuilder());

        assertNotNull(searchRequest);
        processor.processRequest(searchRequest);
        assertSearchRequest(searchRequest);
    }

    public void testScriptingWithPrecompiledIngestScript() throws Exception {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, searchScript, scriptService);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(createSearchSourceBuilder());

        assertNotNull(searchRequest);
        processor.processRequest(searchRequest);
        assertSearchRequest(searchRequest);
    }

    private SearchSourceBuilder createSearchSourceBuilder() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.from(10);
        source.size(20);
        source.explain(true);
        source.version(true);
        source.seqNoAndPrimaryTerm(true);
        source.trackScores(true);
        source.trackTotalHitsUpTo(3);
        source.minScore(1.0f);
        source.timeout(new TimeValue(60, TimeUnit.SECONDS));
        source.terminateAfter(5);
        return source;
    }

    private void assertSearchRequest(SearchRequest searchRequest) {
        assertThat(searchRequest.source().from(), is(20));
        assertThat(searchRequest.source().size(), is(30));
        assertThat(searchRequest.source().explain(), is(false));
        assertThat(searchRequest.source().version(), is(false));
        assertThat(searchRequest.source().seqNoAndPrimaryTerm(), is(false));
        assertThat(searchRequest.source().trackScores(), is(false));
        assertThat(searchRequest.source().trackTotalHitsUpTo(), is(4));
        assertThat(searchRequest.source().minScore(), is(2.0f));
        assertThat(searchRequest.source().timeout(), is(new TimeValue(60, TimeUnit.SECONDS)));
        assertThat(searchRequest.source().terminateAfter(), is(6));
    }
}
