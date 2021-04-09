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

package org.opensearch.search.sort;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

public class SortModeTests extends OpenSearchTestCase {

    public void testSortMode() {
        // we rely on these ordinals in serialization, so changing them breaks bwc.
        assertEquals(0, SortMode.MIN.ordinal());
        assertEquals(1, SortMode.MAX.ordinal());
        assertEquals(2, SortMode.SUM.ordinal());
        assertEquals(3, SortMode.AVG.ordinal());
        assertEquals(4, SortMode.MEDIAN.ordinal());

        assertEquals("min", SortMode.MIN.toString());
        assertEquals("max", SortMode.MAX.toString());
        assertEquals("sum", SortMode.SUM.toString());
        assertEquals("avg", SortMode.AVG.toString());
        assertEquals("median", SortMode.MEDIAN.toString());

        for (SortMode mode : SortMode.values()) {
            assertEquals(mode, SortMode.fromString(mode.toString()));
            assertEquals(mode, SortMode.fromString(mode.toString().toUpperCase(Locale.ROOT)));
        }
    }

    public void testParsingFromStringExceptions() {
        Exception e = expectThrows(NullPointerException.class, () -> SortMode.fromString(null));
        assertEquals("input string is null", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> SortMode.fromString("xyz"));
        assertEquals("Unknown SortMode [xyz]", e.getMessage());
    }
}
