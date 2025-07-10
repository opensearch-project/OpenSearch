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

package org.opensearch.test.rest.yaml.section;

import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class MatchAssertionTests extends OpenSearchTestCase {

    public void testNull() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", null);
            matchAssertion.doAssert(null, null);
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert("non-null", null));
        }
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", "non-null");
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert(null, "non-null"));
        }
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", "/exp/");
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert(null, "/exp/"));
        }
    }

    public void testNullInMap() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", singletonMap("a", null));
        matchAssertion.doAssert(singletonMap("a", null), matchAssertion.getExpectedValue());
        AssertionError e = expectThrows(AssertionError.class, () -> matchAssertion.doAssert(emptyMap(), matchAssertion.getExpectedValue()));
        assertThat(e.getMessage(), containsString("expected [null] but not found"));
    }

    public void testEpsilon() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", 0.95, 0.01);
        matchAssertion.doAssert(0.955, 0.95);
        matchAssertion.doAssert(0.945, 0.95);
        expectThrows(AssertionError.class, () -> matchAssertion.doAssert(0.965, 0.95));
        expectThrows(AssertionError.class, () -> matchAssertion.doAssert(0.935, 0.95));
    }

    public void testEpsilonWithDifferentTypes() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", 100L, 1.0);
        matchAssertion.doAssert(100.5, 100L);
        matchAssertion.doAssert(99.5, 100L);
        MatchAssertion matchAssertion2 = new MatchAssertion(xContentLocation, "field", 100.0, 1.0);
        matchAssertion2.doAssert(100, 100.0);
        matchAssertion2.doAssert(99, 100.0);

        expectThrows(AssertionError.class, () -> matchAssertion.doAssert(101.1, 100L));
        expectThrows(AssertionError.class, () -> matchAssertion2.doAssert(98, 100.0));
    }

    public void testEpsilonIsZero() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(location, "field", 5.0, 0.0);
        matchAssertion.doAssert(5.0, 5.0);

        expectThrows(AssertionError.class, () -> matchAssertion.doAssert(5.0000000001, 5.0));
        expectThrows(AssertionError.class, () -> matchAssertion.doAssert(4.9999999999, 5.0));

        MatchAssertion intMatchAssertion = new MatchAssertion(location, "field", 5, 0.0);
        intMatchAssertion.doAssert(5, 5);
        intMatchAssertion.doAssert(5.0, 5);
        expectThrows(AssertionError.class, () -> intMatchAssertion.doAssert(6, 5));
    }

    public void testNegativeEpsilon() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(location, "field", 5.0, -0.01);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> matchAssertion.doAssert(5.0, 5.0));
        assertThat(e.getMessage(), containsString("epsilon must be non-negative, but was: " + -0.01));
    }

    public void testParseEpsilonNotANumber() throws IOException {
        String jsonContent = "{ \"field_to_match\": 42.0, \"epsilon\": \"invalid_epsilon_value\" }";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), jsonContent)) {
            parser.nextToken();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> MatchAssertion.parse(parser));
            assertThat(e.getMessage(), containsString("epsilon must be a number"));
        }
    }
}
