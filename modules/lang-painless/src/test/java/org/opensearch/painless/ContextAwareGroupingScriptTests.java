/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.common.settings.Settings;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.script.ContextAwareGroupingScript;
import org.opensearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ContextAwareGroupingScriptTests extends ScriptTestCase {

    private static PainlessScriptEngine SCRIPT_ENGINE;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Map<ScriptContext<?>, List<Allowlist>> contexts = newDefaultContexts();
        List<Allowlist> allowlists = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        contexts.put(ContextAwareGroupingScript.CONTEXT, allowlists);

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

    public void testContextAwareGroupingScript() {
        String stringConcat = "ctx.value + \"-context-aware\"";
        ContextAwareGroupingScript script = compile(stringConcat);

        assertEquals("value-context-aware", script.execute(Map.of("value", "value")));

        String integerAddition = "String.valueOf(ctx.value / 100)";
        ContextAwareGroupingScript integerAdditionScript = compile(integerAddition);
        assertEquals("2", integerAdditionScript.execute(Map.of("value", 200)));
    }

    private ContextAwareGroupingScript compile(String expression) {
        ContextAwareGroupingScript.Factory factory = getEngine().compile(
            expression,
            expression,
            ContextAwareGroupingScript.CONTEXT,
            Collections.emptyMap()
        );
        return factory.newInstance();
    }
}
