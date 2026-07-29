/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

public class SliceProfileResultTests extends OpenSearchTestCase {

    public static SliceProfileResult createTestItem() {
        final int sliceId = randomIntBetween(0, 16);
        final long sliceNodeTime = randomNonNegativeLong();
        final int numPartitions = randomIntBetween(1, 4);
        final List<SliceProfileResult.PartitionInfo> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            final int min = randomIntBetween(0, 1000);
            // half the time a whole-segment partition (MAX_VALUE sentinel), half a bounded sub-range
            final int max = randomBoolean() ? Integer.MAX_VALUE : min + randomIntBetween(1, 1000);
            partitions.add(new SliceProfileResult.PartitionInfo(randomIntBetween(0, 32), min, max));
        }
        final int numTimings = randomIntBetween(1, 6);
        final Map<String, Long> breakdown = new LinkedHashMap<>();
        for (int i = 0; i < numTimings; i++) {
            breakdown.put(randomAlphaOfLengthBetween(3, 10) + "_" + i, randomNonNegativeLong());
        }
        return new SliceProfileResult(sliceId, sliceNodeTime, partitions, breakdown);
    }

    private static void assertPartitionsEqual(List<SliceProfileResult.PartitionInfo> a, List<SliceProfileResult.PartitionInfo> b) {
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).getSegmentOrd(), b.get(i).getSegmentOrd());
            assertEquals(a.get(i).getMinDocId(), b.get(i).getMinDocId());
            assertEquals(a.get(i).getMaxDocId(), b.get(i).getMaxDocId());
        }
    }

    public void testStreamRoundTrip() throws IOException {
        final SliceProfileResult original = createTestItem();
        final SliceProfileResult copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new SliceProfileResult(in);
            }
        }
        assertEquals(original.getSliceId(), copy.getSliceId());
        assertEquals(original.getSliceNodeTime(), copy.getSliceNodeTime());
        assertPartitionsEqual(original.getPartitions(), copy.getPartitions());
        assertEquals(original.getBreakdown(), copy.getBreakdown());
    }

    public void testXContentRoundTrip() throws IOException {
        final SliceProfileResult original = createTestItem();
        final BytesReference bytes;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytes = BytesReference.bytes(builder);
        }

        final SliceProfileResult parsed;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = SliceProfileResult.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(original.getSliceId(), parsed.getSliceId());
        assertEquals(original.getSliceNodeTime(), parsed.getSliceNodeTime());
        assertPartitionsEqual(original.getPartitions(), parsed.getPartitions());
        assertEquals(original.getBreakdown(), parsed.getBreakdown());
    }
}
