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

package org.opensearch.ingest;

import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.settings.Settings;
import org.opensearch.script.IngestConditionalScript;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.MockScriptService;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptException;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.StoredScriptSource;
import org.opensearch.test.OpenSearchTestCase;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConditionalProcessorTests extends OpenSearchTestCase {

    private static final String scriptName = "conditionalScript";

    public void testChecksCondition() throws Exception {
        String conditionalField = "field1";
        String scriptName = "conditionalScript";
        String trueValue = "truthy";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(scriptName, ctx -> trueValue.equals(ctx.get(conditionalField))),
                    Collections.emptyMap()
                )
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1), 0L, TimeUnit.MILLISECONDS.toNanos(2));
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            "description",
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
            scriptService,
            new Processor() {
                @Override
                public IngestDocument execute(final IngestDocument ingestDocument) {
                    if (ingestDocument.hasField("error")) {
                        throw new RuntimeException("error");
                    }
                    ingestDocument.setFieldValue("foo", "bar");
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public String getTag() {
                    return null;
                }

                @Override
                public String getDescription() {
                    return null;
                }
            },
            relativeTimeProvider
        );

        // false, never call processor never increments metrics
        String falseValue = "falsy";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, falseValue);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(ingestDocument.getSourceAndMetadata().get(conditionalField), is(falseValue));
        assertThat(ingestDocument.getSourceAndMetadata(), not(hasKey("foo")));
        assertStats(processor, 0, 0, 0);
        assertEquals(scriptName, processor.getCondition());

        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, falseValue);
        ingestDocument.setFieldValue("error", true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertStats(processor, 0, 0, 0);

        // true, always call processor and increments metrics
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, trueValue);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(ingestDocument.getSourceAndMetadata().get(conditionalField), is(trueValue));
        assertThat(ingestDocument.getSourceAndMetadata().get("foo"), is("bar"));
        assertStats(processor, 1, 0, 1);

        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, trueValue);
        ingestDocument.setFieldValue("error", true);
        IngestDocument finalIngestDocument = ingestDocument;
        Exception holder[] = new Exception[1];
        processor.execute(finalIngestDocument, (result, e) -> { holder[0] = e; });
        assertThat(holder[0], instanceOf(RuntimeException.class));
        assertStats(processor, 2, 1, 2);
    }

    @SuppressWarnings("unchecked")
    public void testActsOnImmutableData() throws Exception {
        assertMutatingCtxThrows(ctx -> ctx.remove("foo"));
        assertMutatingCtxThrows(ctx -> ctx.put("foo", "bar"));
        assertMutatingCtxThrows(ctx -> ((List<Object>) ctx.get("listField")).add("bar"));
        assertMutatingCtxThrows(ctx -> ((List<Object>) ctx.get("listField")).remove("bar"));
    }

    public void testPrecompiledError() {
        ScriptService scriptService = MockScriptService.singleContext(IngestConditionalScript.CONTEXT, code -> {
            throw new ScriptException("bad script", new ParseException("error", 0), List.of(), "", "lang", null);
        }, Map.of());
        Script script = new Script(ScriptType.INLINE, "lang", "foo", Map.of());
        ScriptException e = expectThrows(ScriptException.class, () -> new ConditionalProcessor(null, null, script, scriptService, null));
        assertThat(e.getMessage(), equalTo("bad script"));
    }

    public void testRuntimeCompileError() {
        AtomicBoolean fail = new AtomicBoolean(false);
        Map<String, StoredScriptSource> storedScripts = new HashMap<>();
        storedScripts.put("foo", new StoredScriptSource("lang", "", Map.of()));
        ScriptService scriptService = MockScriptService.singleContext(IngestConditionalScript.CONTEXT, code -> {
            if (fail.get()) {
                throw new ScriptException("bad script", new ParseException("error", 0), List.of(), "", "lang", null);
            } else {
                return params -> new IngestConditionalScript(params) {
                    @Override
                    public boolean execute(Map<String, Object> ctx) {
                        return false;
                    }
                };
            }
        }, storedScripts);
        Script script = new Script(ScriptType.STORED, null, "foo", Map.of());
        ConditionalProcessor processor = new ConditionalProcessor(null, null, script, scriptService, null);
        fail.set(true);
        // must change the script source or the cached version will be used
        storedScripts.put("foo", new StoredScriptSource("lang", "changed", Map.of()));
        IngestDocument ingestDoc = new IngestDocument(Map.of(), Map.of());
        processor.execute(ingestDoc, (doc, e) -> { assertThat(e.getMessage(), equalTo("bad script")); });
    }

    public void testRuntimeError() {
        ScriptService scriptService = MockScriptService.singleContext(
            IngestConditionalScript.CONTEXT,
            code -> params -> new IngestConditionalScript(params) {
                @Override
                public boolean execute(Map<String, Object> ctx) {
                    throw new IllegalArgumentException("runtime problem");
                }
            },
            Map.of()
        );
        Script script = new Script(ScriptType.INLINE, "lang", "foo", Map.of());
        ConditionalProcessor processor = new ConditionalProcessor(null, null, script, scriptService, null);
        IngestDocument ingestDoc = new IngestDocument(Map.of(), Map.of());
        processor.execute(ingestDoc, (doc, e) -> { assertThat(e.getMessage(), equalTo("runtime problem")); });
    }

    private static void assertMutatingCtxThrows(Consumer<Map<String, Object>> mutation) throws Exception {
        String scriptName = "conditionalScript";
        CompletableFuture<Exception> expectedException = new CompletableFuture<>();
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    try {
                        mutation.accept(ctx);
                    } catch (Exception e) {
                        expectedException.complete(e);
                    }
                    return false;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            "desription",
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
            scriptService,
            null
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue("listField", new ArrayList<>());
        processor.execute(ingestDocument, (result, e) -> {});
        Exception e = expectedException.get();
        assertThat(e, instanceOf(UnsupportedOperationException.class));
        assertEquals("Mutating ingest documents in conditionals is not supported", e.getMessage());
        assertStats(processor, 0, 0, 0);
    }

    private static void assertStats(ConditionalProcessor conditionalProcessor, long count, long failed, long time) {
        OperationStats stats = conditionalProcessor.getMetric().createStats();
        assertThat(stats.getCount(), equalTo(count));
        assertThat(stats.getCurrent(), equalTo(0L));
        assertThat(stats.getFailedCount(), equalTo(failed));
        assertThat(stats.getTotalTime(), greaterThanOrEqualTo(time));
    }
}
