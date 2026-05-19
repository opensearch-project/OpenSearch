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
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.script.NumberSortScript;
import org.opensearch.script.ScriptContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test that needsScores() is reported correctly depending on whether _score is used
 */
// TODO: can we test this better? this is a port of the ExpressionsTests method.
public class NeedsScoreTests extends OpenSearchSingleNodeTestCase {

    public void testNeedsScores() {
        IndexService index = createIndexWithSimpleMappings("test", Settings.EMPTY, "d", "type=double");

        Map<ScriptContext<?>, List<Allowlist>> contexts = new HashMap<>();
        contexts.put(NumberSortScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        PainlessScriptEngine service = new PainlessScriptEngine(Settings.EMPTY, contexts);

        QueryShardContext shardContext = index.newQueryShardContext(0, null, () -> 0, null);

        NumberSortScript.Factory factory = service.compile(null, "1.2", NumberSortScript.CONTEXT, Collections.emptyMap());
        NumberSortScript.LeafFactory ss = factory.newFactory(Collections.emptyMap(), shardContext.lookup());
        assertFalse(ss.needs_score());

        factory = service.compile(null, "doc['d'].value", NumberSortScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), shardContext.lookup());
        assertFalse(ss.needs_score());

        factory = service.compile(null, "1/_score", NumberSortScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), shardContext.lookup());
        assertTrue(ss.needs_score());

        factory = service.compile(null, "doc['d'].value * _score", NumberSortScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), shardContext.lookup());
        assertTrue(ss.needs_score());
    }
}
