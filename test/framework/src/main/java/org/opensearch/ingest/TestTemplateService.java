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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.ingest;

import org.opensearch.common.settings.Settings;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.Collections;
import java.util.Map;

import static org.opensearch.script.Script.DEFAULT_TEMPLATE_LANG;

public class TestTemplateService extends ScriptService {
    private boolean compilationException;

    public static ScriptService instance() {
        return new TestTemplateService(false);
    }

    public static ScriptService instance(boolean compilationException) {
        return new TestTemplateService(compilationException);
    }

    private TestTemplateService(boolean compilationException) {
        super(Settings.EMPTY, Collections.singletonMap(DEFAULT_TEMPLATE_LANG, new MockScriptEngine()), Collections.emptyMap());
        this.compilationException = compilationException;
    }

    @Override
    public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
        if (this.compilationException) {
            throw new RuntimeException("could not compile script");
        } else {
            return (FactoryType) new MockTemplateScript.Factory(script.getIdOrCode());
        }
    }

    public static class MockTemplateScript extends TemplateScript {
        private final String expected;

        MockTemplateScript(String expected) {
            super(Collections.emptyMap());
            this.expected = expected;
        }

        @Override
        public String execute() {
            return expected;
        }

        public static class Factory implements TemplateScript.Factory {

            private final String expected;

            public Factory(String expected) {
                this.expected = expected;
            }

            @Override
            public TemplateScript newInstance(Map<String, Object> params) {
                return new MockTemplateScript(expected);
            }
        }
    }
}
