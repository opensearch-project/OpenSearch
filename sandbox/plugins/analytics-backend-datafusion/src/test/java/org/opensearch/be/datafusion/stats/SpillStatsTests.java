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
        SpillStats original = new SpillStats("/mnt/spill", 536_870_912_000L, 214_748_364_800L, 1_073_741_824L, 429_496_729_600L, false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        try (StreamInput in = out.bytes().streamInput()) {
            SpillStats roundTripped = new SpillStats(in);
            assertEquals(original, roundTripped);
            assertFalse(roundTripped.isDirectoryWritable());
        }
    }

    public void testToXContentRendersAllFields() throws IOException {
        SpillStats stats = new SpillStats("/mnt/spill", 536_870_912_000L, 214_748_364_800L, 1_073_741_824L, 429_496_729_600L, true);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue("missing spill object key: " + json, json.contains("\"spill\":{"));
        assertTrue(json.contains("\"directory\":\"/mnt/spill\""));
        assertTrue(json.contains("\"disk_total_bytes\":536870912000"));
        assertTrue(json.contains("\"disk_available_bytes\":214748364800"));
        assertTrue(json.contains("\"disk_used_bytes\":1073741824"));
        assertTrue(json.contains("\"disk_reserved_bytes\":429496729600"));
        assertTrue(json.contains("\"directory_writable\":true"));
    }

    public void testEquality() {
        SpillStats a = new SpillStats("/mnt/spill", 100L, 50L, 25L, 80L, true);
        SpillStats b = new SpillStats("/mnt/spill", 100L, 50L, 25L, 80L, true);
        SpillStats differentDir = new SpillStats("/var/tmp", 100L, 50L, 25L, 80L, true);
        SpillStats differentTotal = new SpillStats("/mnt/spill", 999L, 50L, 25L, 80L, true);
        SpillStats differentWritable = new SpillStats("/mnt/spill", 100L, 50L, 25L, 80L, false);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, differentDir);
        assertNotEquals(a, differentTotal);
        assertNotEquals(a, differentWritable);
    }

    public void testDisabledSpillIsVacuouslyWritable() {
        SpillStats disabled = new SpillStats("", 0L, 0L, 0L, 0L, true);
        assertTrue(disabled.isDirectoryWritable());
        assertEquals("", disabled.getDirectory());
    }

    public void testDisabledStateIsRepresentableWithEmptyDirectoryAndZeroBytes() throws IOException {
        SpillStats disabled = new SpillStats("", 0L, 0L, 0L, 0L, true);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        disabled.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"directory\":\"\""));
        assertTrue(json.contains("\"disk_total_bytes\":0"));
    }
}
