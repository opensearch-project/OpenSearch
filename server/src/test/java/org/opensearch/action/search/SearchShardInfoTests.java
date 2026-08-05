/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SearchShardInfoTests extends OpenSearchTestCase {

    public void testEmpty() {
        SearchShardInfo info = SearchShardInfo.EMPTY;
        assertTrue(info.isEmpty());
        assertTrue(info.getSuccessful().isEmpty());
        assertTrue(info.getSkipped().isEmpty());
        assertTrue(info.getFailed().isEmpty());
    }

    public void testStreamRoundTrip() throws IOException {
        SearchShardInfo original = randomInfo();
        SearchShardInfo copy = streamCopy(original);
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
    }

    public void testXContentRoundTrip() throws IOException {
        SearchShardInfo original = randomInfo();
        XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(builder)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(SearchShardInfo.SHARD_INFO_FIELD, parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            SearchShardInfo parsed = SearchShardInfo.fromXContent(parser);
            assertEquals(original, parsed);
        }
    }

    public void testOptionalEntryFieldsAreOmittedFromXContent() throws IOException {
        SearchShardInfo info = new SearchShardInfo(
            Collections.emptyList(),
            List.of(new SearchShardInfo.Entry.Builder("idx", 3).build()),
            Collections.emptyList()
        );
        XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder();
        builder.startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = BytesReference.bytes(builder).utf8ToString();
        assertEquals("{\"shard_info\":{\"successful\":[],\"skipped\":[{\"index\":\"idx\",\"shard\":3}],\"failed\":[]}}", json);
    }

    public void testEntriesAreSortedCanonically() {
        SearchShardInfo.Entry localA0n1 = entry(null, "a", 0, "n1");
        SearchShardInfo.Entry localA0n2 = entry(null, "a", 0, "n2");
        SearchShardInfo.Entry localB0 = entry(null, "b", 0, "n1");
        SearchShardInfo.Entry remoteA0 = entry("remote1", "a", 0, "n1");

        SearchShardInfo info = new SearchShardInfo(
            List.of(remoteA0, localB0, localA0n2, localA0n1),
            Collections.emptyList(),
            Collections.emptyList()
        );
        // cluster alias sorts first (local entries before remote ones), then index, shard, node id
        assertEquals(List.of(localA0n1, localA0n2, localB0, remoteA0), info.getSuccessful());
    }

    public void testEqualsIgnoresConstructionOrder() {
        List<SearchShardInfo.Entry> entries = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 6); i++) {
            entries.add(randomEntry());
        }
        List<SearchShardInfo.Entry> shuffled = new ArrayList<>(entries);
        Collections.shuffle(shuffled, random());

        SearchShardInfo a = new SearchShardInfo(entries, Collections.emptyList(), Collections.emptyList());
        SearchShardInfo b = new SearchShardInfo(shuffled, Collections.emptyList(), Collections.emptyList());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testEntryEqualsAndHashCode() {
        SearchShardInfo.Entry a = new SearchShardInfo.Entry.Builder("idx", 0).nodeId("n1")
            .nodeName("node-one")
            .primary(true)
            .state("STARTED")
            .cluster("remote1")
            .build();
        SearchShardInfo.Entry b = new SearchShardInfo.Entry.Builder("idx", 0).nodeId("n1")
            .nodeName("node-one")
            .primary(true)
            .state("STARTED")
            .cluster("remote1")
            .build();
        SearchShardInfo.Entry different = new SearchShardInfo.Entry.Builder("idx", 1).nodeId("n1").build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, different);
    }

    public void testIsImmutable() throws IOException {
        List<SearchShardInfo.Entry> mutableInput = new ArrayList<>();
        mutableInput.add(new SearchShardInfo.Entry.Builder("idx", 0).nodeId("n1").build());
        SearchShardInfo info = new SearchShardInfo(mutableInput, Collections.emptyList(), Collections.emptyList());
        // Mutating the input list must not change the stored state
        mutableInput.clear();
        assertEquals(1, info.getSuccessful().size());

        SearchShardInfo.Entry extra = new SearchShardInfo.Entry.Builder("idx", 9).build();
        expectThrows(UnsupportedOperationException.class, () -> info.getSuccessful().add(extra));
        expectThrows(UnsupportedOperationException.class, () -> info.getSkipped().add(extra));
        expectThrows(UnsupportedOperationException.class, () -> info.getFailed().add(extra));

        // The wire construction path must expose the same immutability as the list-based one
        SearchShardInfo copy = streamCopy(info);
        expectThrows(UnsupportedOperationException.class, () -> copy.getSuccessful().add(extra));
        expectThrows(UnsupportedOperationException.class, () -> copy.getSkipped().add(extra));
        expectThrows(UnsupportedOperationException.class, () -> copy.getFailed().add(extra));
    }

    public void testMergeIgnoresNullsAndIsOrderIndependent() {
        SearchShardInfo a = randomInfo();
        SearchShardInfo b = randomInfo();
        SearchShardInfo c = randomInfo();

        SearchShardInfo merged = SearchShardInfo.merge(Arrays.asList(a, null, b, c));
        SearchShardInfo mergedShuffled = SearchShardInfo.merge(Arrays.asList(c, b, null, a));
        assertEquals(merged, mergedShuffled);
        assertEquals(a.getSuccessful().size() + b.getSuccessful().size() + c.getSuccessful().size(), merged.getSuccessful().size());
        assertEquals(a.getSkipped().size() + b.getSkipped().size() + c.getSkipped().size(), merged.getSkipped().size());
        assertEquals(a.getFailed().size() + b.getFailed().size() + c.getFailed().size(), merged.getFailed().size());

        assertEquals(SearchShardInfo.EMPTY, SearchShardInfo.merge(Collections.emptyList()));
        assertEquals(SearchShardInfo.EMPTY, SearchShardInfo.merge(Arrays.asList(null, null)));
        assertEquals(SearchShardInfo.EMPTY, SearchShardInfo.merge(List.of(SearchShardInfo.EMPTY)));
    }

    public void testWithClusterAlias() {
        SearchShardInfo.Entry unstamped = new SearchShardInfo.Entry.Builder("idx", 0).nodeId("n1").build();
        SearchShardInfo.Entry alreadyStamped = new SearchShardInfo.Entry.Builder("idx", 1).nodeId("n2").cluster("other").build();
        SearchShardInfo info = new SearchShardInfo(List.of(unstamped, alreadyStamped), Collections.emptyList(), Collections.emptyList());

        SearchShardInfo stamped = info.withClusterAlias("remote1");
        SearchShardInfo expected = new SearchShardInfo(
            List.of(new SearchShardInfo.Entry.Builder("idx", 0).nodeId("n1").cluster("remote1").build(), alreadyStamped),
            Collections.emptyList(),
            Collections.emptyList()
        );
        assertEquals(expected, stamped);

        // null and empty aliases (the local cluster group key) are no-ops
        assertSame(info, info.withClusterAlias(null));
        assertSame(info, info.withClusterAlias(""));
        assertSame(SearchShardInfo.EMPTY, SearchShardInfo.EMPTY.withClusterAlias("remote1"));
    }

    private static SearchShardInfo streamCopy(SearchShardInfo original) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new SearchShardInfo(in);
            }
        }
    }

    private static SearchShardInfo.Entry entry(String cluster, String index, int shard, String nodeId) {
        return new SearchShardInfo.Entry.Builder(index, shard).nodeId(nodeId).cluster(cluster).build();
    }

    private static SearchShardInfo randomInfo() {
        return new SearchShardInfo(randomEntries(4), randomEntries(3), randomEntries(2));
    }

    private static List<SearchShardInfo.Entry> randomEntries(int maxCount) {
        int count = randomIntBetween(0, maxCount);
        List<SearchShardInfo.Entry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(randomEntry());
        }
        return entries;
    }

    private static SearchShardInfo.Entry randomEntry() {
        SearchShardInfo.Entry.Builder builder = new SearchShardInfo.Entry.Builder("idx-" + randomAlphaOfLength(4), randomIntBetween(0, 5));
        if (randomBoolean()) {
            builder.nodeId("node-" + randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            builder.nodeName(randomAlphaOfLength(6));
        }
        if (randomBoolean()) {
            builder.primary(randomBoolean());
        }
        if (randomBoolean()) {
            builder.state(randomFrom("STARTED", "RELOCATING", "INITIALIZING"));
        }
        if (randomBoolean()) {
            builder.cluster("cluster-" + randomAlphaOfLength(3));
        }
        return builder.build();
    }
}
