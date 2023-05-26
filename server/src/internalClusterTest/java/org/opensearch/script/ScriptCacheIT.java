/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.index.mapper.MockFieldFilterPlugin;
import org.opensearch.node.NodeMocksPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.MockSearchService;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.apache.logging.log4j.core.util.Throwables.getRootCause;

public class ScriptCacheIT extends OpenSearchIntegTestCase {

    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.USE_CONTEXT_RATE_KEY);
        // Putting max_compilation_rate for each context to 0 per minute
        for (String s : ScriptModule.CORE_CONTEXTS.keySet()) {
            builder.put("script.context." + s + ".max_compilations_rate", "0/1m");
        }
        return builder.build();
    }

    // Overriding to remove MockScriptService.TestPlugin from the list of plugins
    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (randomBoolean()) {
            if (randomBoolean() && addMockTransportService()) {
                mocks.add(MockTransportService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFSIndexStore.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(NodeMocksPlugin.class);
            }
            if (addMockInternalEngine() && randomBoolean()) {
                mocks.add(MockEngineFactoryPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockSearchService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }
        if (addMockTransportService()) {
            mocks.add(getTestTransportPlugin());
        }
        if (addMockHttpTransport()) {
            mocks.add(MockHttpTransport.TestPlugin.class);
        }
        mocks.add(TestSeedPlugin.class);
        mocks.add(AssertActionNamePlugin.class);
        if (addMockGeoShapeFieldMapper()) {
            mocks.add(TestGeoShapeFieldMapperPlugin.class);
        }
        return Collections.unmodifiableList(mocks);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public void testPainlessCompilationLimit429Error() throws Exception {
        client().prepareIndex("test").setId("1").setSource(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject()).get();
        ensureGreen();
        Map<String, Object> params = new HashMap<>();
        params.put("field", "field");
        Script script = new Script(ScriptType.INLINE, "mockscript", "increase_field", params);
        ExecutionException exception = expectThrows(
            ExecutionException.class,
            () -> client().prepareUpdate("test", "1").setScript(script).execute().get()
        );
        Throwable rootCause = getRootCause(exception);
        assertTrue(rootCause instanceof OpenSearchException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) rootCause).status());
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("increase_field", vars -> {
                Map<String, Object> params = (Map<String, Object>) vars.get("params");
                String fieldname = (String) vars.get("field");
                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                assertNotNull(ctx);
                Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                Number currentValue = (Number) source.get(fieldname);
                Number inc = (Number) params.getOrDefault("inc", 1);
                source.put(fieldname, currentValue.longValue() + inc.longValue());
                return ctx;
            });
            return scripts;
        }
    }
}
