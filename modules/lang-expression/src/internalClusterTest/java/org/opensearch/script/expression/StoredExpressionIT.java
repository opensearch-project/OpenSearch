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

package org.opensearch.script.expression;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

//TODO: please convert to unit tests!
public class StoredExpressionIT extends OpenSearchIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put("script.allowed_contexts", "update");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ExpressionModulePlugin.class);
    }

    public void testAllOpsDisabledIndexedScripts() throws IOException {
        client().admin()
            .cluster()
            .preparePutStoredScript()
            .setId("script1")
            .setContent(new BytesArray("{\"script\": {\"lang\": \"expression\", \"source\": \"2\"} }"), XContentType.JSON)
            .get();
        client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON).get();
        try {
            client().prepareUpdate("test", "1").setScript(new Script(ScriptType.STORED, null, "script1", Collections.emptyMap())).get();
            fail("update script should have been rejected");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("failed to execute script"));
            assertThat(e.getCause().getMessage(), containsString("Failed to compile stored script [script1] using lang [expression]"));
        }
        try {
            client().prepareSearch()
                .setSource(
                    new SearchSourceBuilder().scriptField("test1", new Script(ScriptType.STORED, null, "script1", Collections.emptyMap()))
                )
                .setIndices("test")
                .get();
            fail("search script should have been rejected");
        } catch (Exception e) {
            assertThat(e.toString(), containsString("cannot execute scripts using [field] context"));
        }
        try {
            client().prepareSearch("test")
                .setSource(
                    new SearchSourceBuilder().aggregation(
                        AggregationBuilders.terms("test").script(new Script(ScriptType.STORED, null, "script1", Collections.emptyMap()))
                    )
                )
                .get();
        } catch (Exception e) {
            assertThat(e.toString(), containsString("cannot execute scripts using [aggs] context"));
        }
    }
}
