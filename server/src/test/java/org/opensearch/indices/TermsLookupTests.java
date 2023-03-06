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

package org.opensearch.indices;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TermsLookupTests extends OpenSearchTestCase {
    public void testTermsLookup() {
        String index = randomAlphaOfLengthBetween(1, 10);
        String id = randomAlphaOfLengthBetween(1, 10);
        String path = randomAlphaOfLengthBetween(1, 10);
        String routing = randomAlphaOfLengthBetween(1, 10);
        TermsLookup termsLookup = new TermsLookup(index, id, path);
        termsLookup.routing(routing);
        assertEquals(index, termsLookup.index());
        assertEquals(id, termsLookup.id());
        assertEquals(path, termsLookup.path());
        assertEquals(routing, termsLookup.routing());
    }

    public void testIllegalArguments() {
        String id = randomAlphaOfLength(5);
        String path = randomAlphaOfLength(5);
        String index = randomAlphaOfLength(5);
        switch (randomIntBetween(0, 2)) {
            case 0:
                id = null;
                break;
            case 1:
                path = null;
                break;
            case 2:
                index = null;
                break;
            default:
                fail("unknown case");
        }
        try {
            new TermsLookup(index, id, path);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[terms] query lookup element requires specifying"));
        }
    }

    public void testSerialization() throws IOException {
        TermsLookup termsLookup = randomTermsLookup();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            termsLookup.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                TermsLookup deserializedLookup = new TermsLookup(in);
                assertEquals(deserializedLookup, termsLookup);
                assertEquals(deserializedLookup.hashCode(), termsLookup.hashCode());
                assertNotSame(deserializedLookup, termsLookup);
            }
        }
    }

    public void testXContentParsing() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"index\" : \"index\", \"id\" : \"id\", \"path\" : \"path\", \"routing\" : \"routing\" }"
        );

        TermsLookup tl = TermsLookup.parseTermsLookup(parser);
        assertEquals("index", tl.index());
        assertEquals("id", tl.id());
        assertEquals("path", tl.path());
        assertEquals("routing", tl.routing());
    }

    public static TermsLookup randomTermsLookup() {
        return new TermsLookup(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10).replace('.', '_')).routing(
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }
}
