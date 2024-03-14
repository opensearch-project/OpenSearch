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

package org.opensearch.ingest.common;

import org.opensearch.common.settings.Settings;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.script.IngestScript;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;

public class ScriptProcessorTests extends OpenSearchTestCase {

    private ScriptService scriptService;
    private Script script;
    private IngestScript ingestScript;

    @Before
    public void setupScripting() {
        String scriptName = "script";
        scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    Integer bytesIn = (Integer) ctx.get("bytes_in");
                    Integer bytesOut = (Integer) ctx.get("bytes_out");
                    ctx.put("bytes_total", bytesIn + bytesOut);
                    return null;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap());
        ingestScript = scriptService.compile(script, IngestScript.CONTEXT).newInstance(script.getParams());
    }

    public void testScriptingWithoutPrecompiledScriptFactory() throws Exception {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    public void testScriptingWithPrecompiledIngestScript() {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, ingestScript, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    private IngestDocument randomDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("bytes_in", randomInt());
        document.put("bytes_out", randomInt());
        return RandomDocumentPicks.randomIngestDocument(random(), document);
    }

    private void assertIngestDocument(IngestDocument ingestDocument) {
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_in"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_out"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_total"));
        int bytesTotal = ingestDocument.getFieldValue("bytes_in", Integer.class) + ingestDocument.getFieldValue("bytes_out", Integer.class);
        assertThat(ingestDocument.getSourceAndMetadata().get("bytes_total"), is(bytesTotal));
    }

    public void testScriptingWithSelfReferencingSourceMetadata() {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        IngestDocument originalIngestDocument = randomDocument();
        String index = originalIngestDocument.getSourceAndMetadata().get(IngestDocument.Metadata.INDEX.getFieldName()).toString();
        String id = originalIngestDocument.getSourceAndMetadata().get(IngestDocument.Metadata.ID.getFieldName()).toString();
        Map<String, Object> sourceMetadata = originalIngestDocument.getSourceAndMetadata();
        originalIngestDocument.getSourceAndMetadata().put("_source", sourceMetadata);
        IngestDocument ingestDocument = new IngestDocument(index, id, null, null, null, originalIngestDocument.getSourceAndMetadata());
        expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
    }

}
