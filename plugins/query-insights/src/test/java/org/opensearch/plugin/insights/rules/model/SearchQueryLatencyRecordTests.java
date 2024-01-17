/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Granular tests for the {@link SearchQueryLatencyRecord} class.
 */
public class SearchQueryLatencyRecordTests extends OpenSearchTestCase {

    /**
     * Check that if the serialization, deserialization and equals functions are working as expected
     */
    public void testSerializationAndEquals() throws Exception {
        List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        List<SearchQueryLatencyRecord> copiedRecords = new ArrayList<>();
        for (SearchQueryLatencyRecord record : records) {
            copiedRecords.add(roundTripRecord(record));
        }
        assertTrue(QueryInsightsTestUtils.checkRecordsEquals(records, copiedRecords));

    }

    /**
     * Serialize and deserialize a SearchQueryLatencyRecord.
     * @param record A SearchQueryLatencyRecord to serialize.
     * @return The deserialized, "round-tripped" record.
     */
    private static SearchQueryLatencyRecord roundTripRecord(SearchQueryLatencyRecord record) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            record.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new SearchQueryLatencyRecord(in);
            }
        }
    }
}
