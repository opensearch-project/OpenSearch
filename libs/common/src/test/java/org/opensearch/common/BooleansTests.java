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

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.is;

public class BooleansTests extends OpenSearchTestCase {
    private static final String[] NON_BOOLEANS = new String[] {
        "11",
        "00",
        "sdfsdfsf",
        "F",
        "T",
        "on",
        "off",
        "yes",
        "no",
        "0",
        "1",
        "True",
        "False" };
    private static final String[] BOOLEANS = new String[] { "true", "false" };

    public void testIsBoolean() {
        for (String b : BOOLEANS) {
            String t = "prefix" + b + "suffix";
            assertTrue("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()));
            assertTrue("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(b));
        }
    }

    public void testIsNonBoolean() {
        assertThat(Booleans.isBoolean(null, 0, 1), is(false));

        for (String nb : NON_BOOLEANS) {
            String t = "prefix" + nb + "suffix";
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()));
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t));
        }
    }

    public void testParseBooleanWithFallback() {
        assertFalse(Booleans.parseBoolean(null, false));
        assertTrue(Booleans.parseBoolean(null, true));
        assertNull(Booleans.parseBoolean(null, null));
        assertFalse(Booleans.parseBoolean(null, Boolean.FALSE));
        assertTrue(Booleans.parseBoolean(null, Boolean.TRUE));

        assertFalse(Booleans.parseBoolean("", false));
        assertTrue(Booleans.parseBoolean("", true));
        assertNull(Booleans.parseBoolean("", null));
        assertFalse(Booleans.parseBoolean("", Boolean.FALSE));
        assertTrue(Booleans.parseBoolean("", Boolean.TRUE));

        assertFalse(Booleans.parseBoolean(" \t\n", false));
        assertTrue(Booleans.parseBoolean(" \t\n", true));
        assertNull(Booleans.parseBoolean(" \t\n", null));
        assertFalse(Booleans.parseBoolean(" \t\n", Boolean.FALSE));
        assertTrue(Booleans.parseBoolean(" \t\n", Boolean.TRUE));

        assertTrue(Booleans.parseBoolean("true", randomFrom(Boolean.TRUE, Boolean.FALSE, null)));
        assertFalse(Booleans.parseBoolean("false", randomFrom(Boolean.TRUE, Boolean.FALSE, null)));

        assertTrue(Booleans.parseBoolean(new char[0], 0, 0, true));
        assertFalse(Booleans.parseBoolean(new char[0], 0, 0, false));
    }

    public void testParseNonBooleanWithFallback() {
        for (String nonBoolean : NON_BOOLEANS) {
            boolean defaultValue = randomFrom(Boolean.TRUE, Boolean.FALSE);

            expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(nonBoolean, defaultValue));
            expectThrows(
                IllegalArgumentException.class,
                () -> Booleans.parseBoolean(nonBoolean.toCharArray(), 0, nonBoolean.length(), defaultValue)
            );
        }
    }

    public void testParseBoolean() {
        assertTrue(Booleans.parseBoolean("true"));
        assertFalse(Booleans.parseBoolean("false"));
    }

    public void testParseNonBoolean() {
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(null));
        for (String nonBoolean : NON_BOOLEANS) {
            expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(nonBoolean));
        }
    }

    public void testParseBooleanStrict() {
        assertTrue(Booleans.parseBooleanStrict("true", false));
        assertFalse(Booleans.parseBooleanStrict("false", true));
        assertTrue(Booleans.parseBooleanStrict(null, true));
        assertFalse(Booleans.parseBooleanStrict("", false));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBooleanStrict("foobar", false));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBooleanStrict(" \t\n", false));
    }
}
