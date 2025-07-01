/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AbstractProfileBreakdownTests extends OpenSearchTestCase {
    private enum TestType {
        STAT_1,
        STAT_2;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static class TestProfileBreakdown extends AbstractProfileBreakdown {
        Map<String, Long> stats;

        TestProfileBreakdown() {
            super(List.of());
            stats = new HashMap<String, Long>();
            long counter = 123;
            for (TestType type : TestType.values()) {
                stats.put(type.toString(), counter++);
            }
        }

        @Override
        public Map<String, Long> toBreakdownMap() {
            return Collections.unmodifiableMap(stats);
        }

        @Override
        public Map<String, Object> toDebugMap() {
            return Map.of("test_debug", 1234L);
        }
    }

    public void testToBreakdownMap() {
        AbstractProfileBreakdown breakdown = new TestProfileBreakdown();
        Map<String, Long> stats = new HashMap<>();
        stats.put("stat_1", 123L);
        stats.put("stat_2", 124L);
        assertEquals(stats, breakdown.toBreakdownMap());
        assertEquals(Collections.singletonMap("test_debug", 1234L), breakdown.toDebugMap());
    }
}
