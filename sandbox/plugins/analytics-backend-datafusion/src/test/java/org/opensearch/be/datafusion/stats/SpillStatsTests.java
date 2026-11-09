/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class SpillStatsTests extends OpenSearchTestCase {

    public void testWireFormatRoundTrip() throws IOException {
        SpillStats original = new SpillStats("/mnt/spill", 536_870_912_000L, 214_748_364_800L, 1_073_741_824L, 429_496_729_600L, 7L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        try (StreamInput in = out.bytes().streamInput()) {
            SpillStats roundTripped = new SpillStats(in);
            assertEquals(original, roundTripped);
            assertEquals(7L, roundTripped.getStaleEntryCount());
        }
    }

    public void testToXContentRendersAllFields() throws IOException {
        SpillStats stats = new SpillStats("/mnt/spill", 536_870_912_000L, 214_748_364_800L, 1_073_741_824L, 429_496_729_600L, 3L);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue("missing disk_spill object key: " + json, json.contains("\"disk_spill\":{"));
        assertTrue(json.contains("\"directory\":\"/mnt/spill\""));
        assertTrue(json.contains("\"disk_total_bytes\":536870912000"));
        assertTrue(json.contains("\"disk_available_bytes\":214748364800"));
        assertTrue(json.contains("\"disk_used_bytes\":1073741824"));
        assertTrue(json.contains("\"disk_reserved_bytes\":429496729600"));
        assertTrue(json.contains("\"stale_entry_count\":3"));
    }

    public void testEquality() {
        SpillStats a = new SpillStats("/mnt/spill", 100L, 50L, 25L, 80L, 0L);
        SpillStats b = new SpillStats("/mnt/spill", 100L, 50L, 25L, 80L, 0L);
        SpillStats differentDir = new SpillStats("/var/tmp", 100L, 50L, 25L, 80L, 0L);
        SpillStats differentTotal = new SpillStats("/mnt/spill", 999L, 50L, 25L, 80L, 0L);
        SpillStats differentStaleCount = new SpillStats("/mnt/spill", 100L, 50L, 25L, 80L, 5L);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, differentDir);
        assertNotEquals(a, differentTotal);
        assertNotEquals("stale_entry_count must participate in equality", a, differentStaleCount);
    }

    public void testDisabledStateIsRepresentableWithEmptyDirectoryAndZeroBytes() throws IOException {
        SpillStats disabled = new SpillStats("", 0L, 0L, 0L, 0L, 0L);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        disabled.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"directory\":\"\""));
        assertTrue(json.contains("\"disk_total_bytes\":0"));
        assertTrue(json.contains("\"stale_entry_count\":0"));
    }
}
