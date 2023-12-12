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

import org.opensearch.common.settings.Settings;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.painless.spi.AllowlistInstanceBinding;
import org.opensearch.painless.spi.AllowlistLoader;
import org.opensearch.script.ScriptContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BindingsTests extends ScriptTestCase {
    private static PainlessScriptEngine SCRIPT_ENGINE;

    @BeforeClass
    public static void beforeClass() {
        Map<ScriptContext<?>, List<Allowlist>> contexts = newDefaultContexts();
        List<Allowlist> allowlists = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        allowlists.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.opensearch.painless.test"));

        InstanceBindingTestClass instanceBindingTestClass = new InstanceBindingTestClass(1);
        AllowlistInstanceBinding getter = new AllowlistInstanceBinding(
            "test",
            instanceBindingTestClass,
            "setInstanceBindingValue",
            "void",
            Collections.singletonList("int"),
            Collections.emptyList()
        );
        AllowlistInstanceBinding setter = new AllowlistInstanceBinding(
            "test",
            instanceBindingTestClass,
            "getInstanceBindingValue",
            "int",
            Collections.emptyList(),
            Collections.emptyList()
        );
        List<AllowlistInstanceBinding> instanceBindingsList = new ArrayList<>();
        instanceBindingsList.add(getter);
        instanceBindingsList.add(setter);
        Allowlist instanceBindingsAllowlist = new Allowlist(
            instanceBindingTestClass.getClass().getClassLoader(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            instanceBindingsList
        );
        allowlists.add(instanceBindingsAllowlist);

        contexts.put(BindingsTestScript.CONTEXT, allowlists);
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

    public static class BindingTestClass {
        public int state;

        public BindingTestClass(int state0, int state1) {
            this.state = state0 + state1;
        }

        public int addWithState(int istateless, double dstateless) {
            return istateless + state + (int) dstateless;
        }
    }

    public static class ThisBindingTestClass {
        private BindingsTestScript bindingsTestScript;
        private int state;

        public ThisBindingTestClass(BindingsTestScript bindingsTestScript, int state0, int state1) {
            this.bindingsTestScript = bindingsTestScript;
            this.state = state0 + state1;
        }

        public int addThisWithState(int istateless, double dstateless) {
            return istateless + state + (int) dstateless + bindingsTestScript.getTestValue();
        }
    }

    public static class EmptyThisBindingTestClass {
        private BindingsTestScript bindingsTestScript;

        public EmptyThisBindingTestClass(BindingsTestScript bindingsTestScript) {
            this.bindingsTestScript = bindingsTestScript;
        }

        public int addEmptyThisWithState(int istateless) {
            return istateless + bindingsTestScript.getTestValue();
        }
    }

    public static class InstanceBindingTestClass {
        private int value;

        public InstanceBindingTestClass(int value) {
            this.value = value;
        }

        public void setInstanceBindingValue(int value) {
            this.value = value;
        }

        public int getInstanceBindingValue() {
            return value;
        }
    }

    public abstract static class BindingsTestScript {
        public static final String[] PARAMETERS = { "test", "bound" };

        public int getTestValue() {
            return 7;
        }

        public abstract int execute(int test, int bound);

        public interface Factory {
            BindingsTestScript newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("bindings_test", Factory.class);
    }

    public void testBasicClassBinding() {
        String script = "addWithState(4, 5, 6, 0.0)";
        BindingsTestScript.Factory factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(15, executableScript.execute(0, 0));
    }

    public void testRepeatedClassBinding() {
        String script = "addWithState(4, 5, test, 0.0)";
        BindingsTestScript.Factory factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(14, executableScript.execute(5, 0));
        assertEquals(13, executableScript.execute(4, 0));
        assertEquals(16, executableScript.execute(7, 0));
    }

    public void testBoundClassBinding() {
        String script = "addWithState(4, bound, test, 0.0)";
        BindingsTestScript.Factory factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(10, executableScript.execute(5, 1));
        assertEquals(9, executableScript.execute(4, 2));
    }

    public void testThisClassBinding() {
        String script = "addThisWithState(4, bound, test, 0.0)";

        BindingsTestScript.Factory factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(17, executableScript.execute(5, 1));
        assertEquals(16, executableScript.execute(4, 2));
    }

    public void testEmptyThisClassBinding() {
        String script = "addEmptyThisWithState(test)";

        BindingsTestScript.Factory factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();

        assertEquals(8, executableScript.execute(1, 0));
        assertEquals(9, executableScript.execute(2, 0));
    }

    public void testInstanceBinding() {
        String script = "getInstanceBindingValue() + test + bound";
        BindingsTestScript.Factory factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        BindingsTestScript executableScript = factory.newInstance();
        assertEquals(3, executableScript.execute(1, 1));

        script = "setInstanceBindingValue(test + bound); getInstanceBindingValue()";
        factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        executableScript = factory.newInstance();
        assertEquals(4, executableScript.execute(-2, 6));

        script = "getInstanceBindingValue() + test + bound";
        factory = getEngine().compile(null, script, BindingsTestScript.CONTEXT, Collections.emptyMap());
        executableScript = factory.newInstance();
        assertEquals(8, executableScript.execute(-2, 6));
    }
}
