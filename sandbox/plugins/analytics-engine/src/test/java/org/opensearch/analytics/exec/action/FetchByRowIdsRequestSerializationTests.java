/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Wire serialization round-trip tests for {@link FetchByRowIdsRequest}.
 */
public class FetchByRowIdsRequestSerializationTests extends OpenSearchTestCase {

    public void testRoundTripWithProfileTrue() throws IOException {
        FetchByRowIdsRequest original = new FetchByRowIdsRequest(
            "query-123",
            2,
            new ShardId(new Index("test_index", "uuid"), 0),
            "datafusion",
            new long[] { 10, 42, 99 },
            new String[] { "name", "score" },
            true
        );

        FetchByRowIdsRequest deserialized = roundTrip(original);

        assertEquals("query-123", deserialized.getQueryId());
        assertEquals(2, deserialized.getStageId());
        assertEquals("test_index", deserialized.getShardId().getIndexName());
        assertEquals(0, deserialized.getShardId().id());
        assertEquals("datafusion", deserialized.getBackendId());
        assertArrayEquals(new long[] { 10, 42, 99 }, deserialized.getRowIds());
        assertArrayEquals(new String[] { "name", "score" }, deserialized.getColumns());
        assertTrue("profile flag must survive round-trip", deserialized.profile());
    }

    public void testRoundTripWithProfileFalse() throws IOException {
        FetchByRowIdsRequest original = new FetchByRowIdsRequest(
            "query-456",
            0,
            new ShardId(new Index("logs", "uuid2"), 3),
            "lucene",
            new long[] { 0 },
            new String[] { "msg" },
            false
        );

        FetchByRowIdsRequest deserialized = roundTrip(original);

        assertEquals("query-456", deserialized.getQueryId());
        assertEquals(0, deserialized.getStageId());
        assertEquals(3, deserialized.getShardId().id());
        assertEquals("lucene", deserialized.getBackendId());
        assertArrayEquals(new long[] { 0 }, deserialized.getRowIds());
        assertArrayEquals(new String[] { "msg" }, deserialized.getColumns());
        assertFalse("profile=false must survive round-trip", deserialized.profile());
    }

    private FetchByRowIdsRequest roundTrip(FetchByRowIdsRequest original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new FetchByRowIdsRequest(in);
    }
}
