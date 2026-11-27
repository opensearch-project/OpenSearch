/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchModule;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class TermsEnumShardRequestTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()
    );

    private static TermsEnumShardRequest randomRequest() {
        TermsEnumRequest request = new TermsEnumRequest("a", "b").field("host")
            .string(randomAlphaOfLengthBetween(0, 10))
            .size(randomIntBetween(1, 100))
            .caseInsensitive(randomBoolean())
            .indicesOptions(IndicesOptions.lenientExpandOpen());
        if (randomBoolean()) {
            request.searchAfter(randomAlphaOfLength(5));
        }
        if (randomBoolean()) {
            request.indexFilter(QueryBuilders.termQuery("f", "v"));
        }
        TermsEnumShardRequest shardRequest = new TermsEnumShardRequest(
            new ShardId("idx", "uuid", randomIntBetween(0, 5)),
            new OriginalIndices(request),
            request,
            randomNonNegativeLong(),
            randomLongBetween(1, 10_000)
        );
        shardRequest.setParentTask(new TaskId("node", randomIntBetween(1, 100)));
        return shardRequest;
    }

    public void testSerializationRoundTrip() throws Exception {
        TermsEnumShardRequest original = randomRequest();
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        TermsEnumShardRequest copy;
        try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), REGISTRY)) {
            copy = new TermsEnumShardRequest(in);
        }
        assertEquals(original.shardId(), copy.shardId());
        assertEquals(original.field(), copy.field());
        assertEquals(original.string(), copy.string());
        assertEquals(original.size(), copy.size());
        assertEquals(original.caseInsensitive(), copy.caseInsensitive());
        assertEquals(original.searchAfter(), copy.searchAfter());
        assertEquals(original.indexFilter(), copy.indexFilter());
        assertEquals(original.nowInMillis(), copy.nowInMillis());
        assertEquals(original.timeoutMillis(), copy.timeoutMillis());
        assertArrayEquals(original.indices(), copy.indices());
        assertEquals(original.indicesOptions(), copy.indicesOptions());
        assertEquals(original.getParentTask(), copy.getParentTask());
    }

    public void testCreateTaskIsSearchShardTask() {
        Task task = randomRequest().createTask(1L, "transport", "indices:data/read/terms_enum[s]", TaskId.EMPTY_TASK_ID, Map.of());
        assertTrue(task instanceof SearchShardTask);
    }

    public void testIndicesAreOriginalIndices() {
        TermsEnumShardRequest request = randomRequest();
        assertArrayEquals(new String[] { "a", "b" }, request.indices());
        assertEquals(IndicesOptions.lenientExpandOpen(), request.indicesOptions());
        assertEquals("shardId[" + request.shardId() + "]", request.getDescription());
        assertNotNull(TimeValue.timeValueMillis(request.timeoutMillis()));
    }
}
