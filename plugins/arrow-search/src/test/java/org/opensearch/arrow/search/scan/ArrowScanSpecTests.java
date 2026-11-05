/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.arrow.search.Column;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ArrowScanSpecTests extends OpenSearchTestCase {

    static ArrowScanSpec spec(long batchBytes, long firstBatchBytes) {
        return new ArrowScanSpec(
            List.of(new Column("l", Column.ColumnType.INT64, Set.of("long")), new Column("k", Column.ColumnType.UTF8, Set.of("keyword"))),
            batchBytes,
            firstBatchBytes,
            65536,
            3 << 20,
            Long.MAX_VALUE,
            TimeValue.timeValueMinutes(1)
        );
    }

    public void testBatchesRampUpToTheBatchSize() {
        ArrowScanSpec spec = spec(1 << 20, 64 << 10);
        assertEquals(64 << 10, spec.batchBudget(0));
        assertEquals(128 << 10, spec.batchBudget(1));
        assertEquals(512 << 10, spec.batchBudget(3));
        assertEquals(1 << 20, spec.batchBudget(4));
        assertEquals(1 << 20, spec.batchBudget(5));
    }

    public void testRampNeverOverflows() {
        ArrowScanSpec spec = spec(3 << 20, 64 << 10);
        for (int batch = 0; batch < 200; batch++) {
            long budget = spec.batchBudget(batch);
            assertTrue("batch " + batch + " budget " + budget, budget > 0 && budget <= 3 << 20);
        }
        assertEquals(3 << 20, spec.batchBudget(Integer.MAX_VALUE));
    }

    public void testSerialization() throws IOException {
        ArrowScanSpec spec = spec(randomLongBetween(16 << 10, 3 << 20), randomLongBetween(16 << 10, 64 << 10));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            spec.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(spec, new ArrowScanSpec(in));
            }
        }
    }

    public void testSummaryRoundTrips() {
        ShardSummary summary = new ShardSummary(randomNonNegativeLong(), randomBoolean(), randomBoolean());
        assertEquals(summary, ShardSummary.decode(summary.encode()));
    }
}
