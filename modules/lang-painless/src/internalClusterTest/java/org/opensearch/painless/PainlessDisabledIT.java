/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Verifies that Painless scripts are rejected end to end when {@code script.engines.painless.enabled} is {@code false}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class PainlessDisabledIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessModulePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("script.engines.painless.enabled", false).build();
    }

    private void indexDoc() {
        client().prepareIndex("test")
            .setId("1")
            .setSource("{\"value\": 1}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private static final String DISABLED_MESSAGE = "script language [painless] is disabled by [script.engines.painless.enabled]";

    private static void assertDisabled(Exception e) {
        assertTrue("expected [" + DISABLED_MESSAGE + "] in " + e, ExceptionsHelper.stackTrace(e).contains(DISABLED_MESSAGE));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
    }

    public void testSearchWithPainlessScriptIsRejected() {
        indexDoc();
        Script script = new Script(ScriptType.INLINE, "painless", "doc['value'].value * 2", Collections.emptyMap());
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("test").addScriptField("doubled", script).get()
        );
        assertDisabled(e);
    }

    public void testUpdateWithPainlessScriptIsRejected() {
        indexDoc();
        Script script = new Script(ScriptType.INLINE, "painless", "ctx._source.value += 1", Collections.emptyMap());
        Exception e = expectThrows(Exception.class, () -> client().prepareUpdate("test", "1").setScript(script).get());
        assertDisabled(e);
    }

    public void testPutStoredPainlessScriptIsRejected() {
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("disabled")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"painless\", \"source\": \"1\"}}"), MediaTypeRegistry.JSON)
                .get()
        );
        assertDisabled(e);
    }

    public void testSearchWithoutScriptsStillWorks() {
        indexDoc();
        assertEquals(1, client().prepareSearch("test").get().getHits().getTotalHits().value());
    }
}
