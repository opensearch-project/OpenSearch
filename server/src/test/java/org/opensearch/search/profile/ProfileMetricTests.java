/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ProfileMetricTests extends OpenSearchTestCase {

    private static class TestMetric extends ProfileMetric {

        private long value = 0L;

        public TestMetric(String name) {
            super(name);
        }

        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public Map<String, Long> toBreakdownMap() {
            return Map.of("test_metric", value);
        }
    }

    public void testNonTimingMetric() {
        TestMetric test_metric = new TestMetric("test_metric");
        test_metric.setValue(1234L);
        assertEquals(test_metric.getName(), "test_metric");
        Map<String, Long> map = test_metric.toBreakdownMap();
        assertEquals(map.get("test_metric").longValue(), 1234L);
    }

    public static Collection<Supplier<ProfileMetric>> getNonTimingMetric() {
        return List.of(() -> new TestMetric("test_metric"));
    }
}
