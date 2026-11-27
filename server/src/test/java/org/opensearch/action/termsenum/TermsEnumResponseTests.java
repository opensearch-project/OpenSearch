/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class TermsEnumResponseTests extends OpenSearchTestCase {

    private static TermsEnumResponse.ShardFailure failure(String index, int shard) {
        return new TermsEnumResponse.ShardFailure(
            index,
            shard,
            new NoShardAvailableActionException(new ShardId(index, "u", shard), "gone")
        );
    }

    public void testSerializationRoundTrip() throws Exception {
        TermsEnumResponse.ShardFailure[] failures = new TermsEnumResponse.ShardFailure[randomIntBetween(0, 3)];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = failure("idx" + i, i);
        }
        TermsEnumResponse original = new TermsEnumResponse(
            failures.length + randomIntBetween(0, 5),
            randomIntBetween(0, 2),
            failures,
            List.of("a", "b", "c"),
            randomBoolean()
        );
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        TermsEnumResponse copy;
        try (StreamInput in = out.bytes().streamInput()) {
            copy = new TermsEnumResponse(in);
        }
        assertEquals(original.getTotalShards(), copy.getTotalShards());
        assertEquals(original.getSuccessfulShards(), copy.getSuccessfulShards());
        assertEquals(original.getSkippedShards(), copy.getSkippedShards());
        assertEquals(original.getFailedShards(), copy.getFailedShards());
        assertEquals(original.getTerms(), copy.getTerms());
        assertEquals(original.isComplete(), copy.isComplete());
        assertEquals(failures.length, copy.getShardFailures().length);
        for (int i = 0; i < failures.length; i++) {
            TermsEnumResponse.ShardFailure read = copy.getShardFailures()[i];
            assertEquals(failures[i].index(), read.index());
            assertEquals(failures[i].shardId(), read.shardId());
            assertEquals(failures[i].reason(), read.reason());
        }
    }

    public void testToXContent() throws Exception {
        TermsEnumResponse r = new TermsEnumResponse(
            6,
            2,
            new TermsEnumResponse.ShardFailure[0],
            List.of("web-03", "web-04", "web-05"),
            true
        );
        assertEquals(
            "{\"_shards\":{\"total\":6,\"successful\":6,\"skipped\":2,\"failed\":0},"
                + "\"terms\":[\"web-03\",\"web-04\",\"web-05\"],\"complete\":true}",
            Strings.toString(MediaTypeRegistry.JSON, r)
        );
    }

    @SuppressWarnings("unchecked")
    public void testToXContentWithFailure() throws Exception {
        TermsEnumResponse r = new TermsEnumResponse(2, 0, new TermsEnumResponse.ShardFailure[] { failure("logs", 0) }, List.of("a"), false);
        Map<String, Object> map = XContentHelper.convertToMap(
            MediaTypeRegistry.JSON.xContent(),
            Strings.toString(MediaTypeRegistry.JSON, r),
            false
        );
        Map<String, Object> shards = (Map<String, Object>) map.get("_shards");
        assertEquals(1, shards.get("failed"));
        List<Map<String, Object>> failures = (List<Map<String, Object>>) shards.get("failures");
        assertEquals(1, failures.size());
        assertEquals("logs", failures.get(0).get("index"));
        assertEquals(false, map.get("complete"));
    }
}
