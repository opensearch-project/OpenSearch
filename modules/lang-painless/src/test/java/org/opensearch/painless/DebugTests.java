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

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.painless.lookup.PainlessLookup;
import org.opensearch.painless.lookup.PainlessLookupBuilder;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.script.ScriptException;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DebugTests extends ScriptTestCase {
    private final PainlessLookup painlessLookup = PainlessLookupBuilder.buildFromAllowlists(Allowlist.BASE_ALLOWLISTS);

    public void testExplain() {
        // Debug.explain can explain an object
        Object dummy = new Object();
        PainlessExplainError e = expectScriptThrows(
            PainlessExplainError.class,
            () -> exec("Debug.explain(params.a)", singletonMap("a", dummy), true)
        );
        assertSame(dummy, e.getObjectToExplain());
        assertThat(e.getHeaders(painlessLookup), hasEntry("opensearch.to_string", singletonList(dummy.toString())));
        assertThat(e.getHeaders(painlessLookup), hasEntry("opensearch.java_class", singletonList("java.lang.Object")));
        assertThat(e.getHeaders(painlessLookup), hasEntry("opensearch.painless_class", singletonList("java.lang.Object")));

        // Null should be ok
        e = expectScriptThrows(PainlessExplainError.class, () -> exec("Debug.explain(null)"));
        assertNull(e.getObjectToExplain());
        assertThat(e.getHeaders(painlessLookup), hasEntry("opensearch.to_string", singletonList("null")));
        assertThat(e.getHeaders(painlessLookup), not(hasKey("opensearch.java_class")));
        assertThat(e.getHeaders(painlessLookup), not(hasKey("opensearch.painless_class")));

        // You can't catch the explain exception
        e = expectScriptThrows(
            PainlessExplainError.class,
            () -> exec(
                "try {\n" + "  Debug.explain(params.a)\n" + "} catch (Exception e) {\n" + "  return 1\n" + "}",
                singletonMap("a", dummy),
                true
            )
        );
        assertSame(dummy, e.getObjectToExplain());
    }

    /**
     * {@link PainlessExplainError} doesn't serialize but the headers still make it.
     */
    public void testPainlessExplainErrorSerialization() throws IOException {
        Map<String, Object> params = singletonMap("a", "jumped over the moon");
        ScriptException e = expectThrows(ScriptException.class, () -> exec("Debug.explain(params.a)", params, true));
        assertEquals(singletonList("jumped over the moon"), e.getMetadata("opensearch.to_string"));
        assertEquals(singletonList("java.lang.String"), e.getMetadata("opensearch.java_class"));
        assertEquals(singletonList("java.lang.String"), e.getMetadata("opensearch.painless_class"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(e);
            try (StreamInput in = out.bytes().streamInput()) {
                OpenSearchException read = (ScriptException) in.readException();
                assertEquals(singletonList("jumped over the moon"), read.getMetadata("opensearch.to_string"));
                assertEquals(singletonList("java.lang.String"), read.getMetadata("opensearch.java_class"));
                assertEquals(singletonList("java.lang.String"), read.getMetadata("opensearch.painless_class"));
            }
        }
    }
}
