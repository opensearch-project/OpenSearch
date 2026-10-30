/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link SliceAssignment} across all shard-split strategies.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class SliceAssignmentTests extends OpenSearchTestCase {

    private FsBlobStore store;
    private BlobContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Path dataDir = createTempDir().resolve("data");
        Files.createDirectories(dataDir);
        store = new FsBlobStore(8192, dataDir, false);
        container = store.blobContainer(BlobPath.cleanPath());
    }

    private void write(String name, String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(name, in, bytes.length, true);
        }
    }

    private static ObjectStoreSourceConfig config(String split) {
        return new ObjectStoreSourceConfig(Map.of("repository", "repo", "shard_split", split));
    }

    public void testEqualBytesTwoShardsDisjointAndComplete() throws Exception {
        write("big", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("big", 20L));
        ObjectStoreSourceConfig config = config("equal_bytes");

        List<ObjectSlice> shard0 = SliceAssignment.assign(config, manifest, container, null, 0, 2);
        List<ObjectSlice> shard1 = SliceAssignment.assign(config, manifest, container, null, 1, 2);

        assertThat(shard0, contains(new ObjectSlice("big", 0, 15, 20)));
        assertThat(shard1, contains(new ObjectSlice("big", 15, 20, 20)));
    }

    public void testEqualBytesCollapsedRangesYieldEmptyShards() throws Exception {
        write("small", "aa\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("small", 3L));
        ObjectStoreSourceConfig config = config("equal_bytes");

        assertThat(SliceAssignment.assign(config, manifest, container, null, 0, 3), contains(new ObjectSlice("small", 0, 3, 3)));
        assertThat(SliceAssignment.assign(config, manifest, container, null, 1, 3), is(empty()));
        assertThat(SliceAssignment.assign(config, manifest, container, null, 2, 3), is(empty()));
    }

    public void testEqualBytesRejectsMultiObjectManifest() {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 1L, "b", 1L));
        ObjectStoreSourceConfig config = config("equal_bytes");
        expectThrows(IllegalArgumentException.class, () -> SliceAssignment.assign(config, manifest, container, null, 0, 2));
    }

    public void testWholeFilesPartitionsObjectsDisjointlyAndCompletely() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 1L, "b", 2L, "c", 3L, "d", 4L, "e", 5L));
        ObjectStoreSourceConfig config = config("whole_files");
        int numShards = 3;

        Set<String> unionOwned = new HashSet<>();
        for (int shard = 0; shard < numShards; shard++) {
            for (ObjectSlice slice : SliceAssignment.assign(config, manifest, null, null, shard, numShards)) {
                assertThat(
                    "object owned by more than one shard: " + slice.getObjectName(),
                    unionOwned.add(slice.getObjectName()),
                    is(true)
                );
            }
        }
        assertThat(unionOwned, is(new HashSet<>(manifest.keys())));
    }

    public void testWholeFilesAssignmentIsDeterministic() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 1L, "b", 2L, "c", 3L, "d", 4L));
        ObjectStoreSourceConfig config = config("whole_files");

        assertThat(
            SliceAssignment.assign(config, manifest, null, null, 0, 2),
            is(SliceAssignment.assign(config, manifest, null, null, 0, 2))
        );
    }

    public void testWholeFilesFewerObjectsThanShardsLeavesSomeShardsEmpty() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("only", 5L));
        ObjectStoreSourceConfig config = config("whole_files");
        int numShards = 3;

        int ownedCount = 0;
        for (int shard = 0; shard < numShards; shard++) {
            ownedCount += SliceAssignment.assign(config, manifest, null, null, shard, numShards).size();
        }
        assertThat(ownedCount, is(1));
    }

    public void testShardPrefixesWithoutBasePrefix() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("0/x", 1L, "0/y", 2L, "1/z", 3L));
        ObjectStoreSourceConfig config = config("shard_prefixes");

        assertThat(
            SliceAssignment.assign(config, manifest, null, "", 0, 2),
            contains(new ObjectSlice("0/x", 0, 1, 1), new ObjectSlice("0/y", 0, 2, 2))
        );
        assertThat(SliceAssignment.assign(config, manifest, null, "", 1, 2), contains(new ObjectSlice("1/z", 0, 3, 3)));
    }

    public void testShardPrefixesWithBasePrefix() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/1/y", 2L));
        ObjectStoreSourceConfig config = config("shard_prefixes");

        assertThat(SliceAssignment.assign(config, manifest, null, "dump", 0, 2), contains(new ObjectSlice("dump/0/x", 0, 1, 1)));
        assertThat(SliceAssignment.assign(config, manifest, null, "dump/", 1, 2), contains(new ObjectSlice("dump/1/y", 0, 2, 2)));
    }

    public void testShardPrefixesWithNullBasePrefix() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("0/x", 1L, "1/z", 3L));
        ObjectStoreSourceConfig config = config("shard_prefixes");

        assertThat(SliceAssignment.assign(config, manifest, null, null, 0, 2), contains(new ObjectSlice("0/x", 0, 1, 1)));
        assertThat(SliceAssignment.assign(config, manifest, null, null, 1, 2), contains(new ObjectSlice("1/z", 0, 3, 3)));
    }

    public void testShardPrefixesObjectOutsideAnySubPrefixOwnedByNoShard() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/1/y", 2L, "dump/orphan.ndjson", 5L));
        ObjectStoreSourceConfig config = config("shard_prefixes");

        assertThat(SliceAssignment.assign(config, manifest, null, "dump", 0, 2), contains(new ObjectSlice("dump/0/x", 0, 1, 1)));
        assertThat(SliceAssignment.assign(config, manifest, null, "dump", 1, 2), contains(new ObjectSlice("dump/1/y", 0, 2, 2)));
    }

    public void testShardPrefixesUnownedObjectWarnsFromShardZeroOnly() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/orphan.ndjson", 5L));
        ObjectStoreSourceConfig config = config("shard_prefixes");

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SliceAssignment.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "orphan warning from shard 0",
                    SliceAssignment.class.getCanonicalName(),
                    Level.WARN,
                    "*not under any shard sub-prefix*"
                )
            );
            SliceAssignment.assign(config, manifest, null, "dump", 0, 2);
            appender.assertAllExpectationsMatched();
        }

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SliceAssignment.class))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no orphan warning from non-zero shard",
                    SliceAssignment.class.getCanonicalName(),
                    Level.WARN,
                    "*not under any shard sub-prefix*"
                )
            );
            SliceAssignment.assign(config, manifest, null, "dump", 1, 2);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testShardIdOutOfRangeThrows() {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 1L));
        ObjectStoreSourceConfig config = config("whole_files");
        expectThrows(IllegalArgumentException.class, () -> SliceAssignment.assign(config, manifest, null, null, 3, 2));
        expectThrows(IllegalArgumentException.class, () -> SliceAssignment.assign(config, manifest, null, null, -1, 2));
    }

    private void assertOrphanWarned(ObjectStoreManifest manifest, String resolvedPrefix, int numShards) throws Exception {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SliceAssignment.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "orphan warning",
                    SliceAssignment.class.getCanonicalName(),
                    Level.WARN,
                    "*not under any shard sub-prefix*"
                )
            );
            SliceAssignment.assign(config("shard_prefixes"), manifest, null, resolvedPrefix, 0, numShards);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testShardPrefixesObjectOutsideTheBasePrefixWarns() throws Exception {
        assertOrphanWarned(new ObjectStoreManifest(Map.of("dump/0/x", 1L, "elsewhere/0/y", 2L)), "dump", 2);
    }

    public void testShardPrefixesNonNumericSubPrefixWarns() throws Exception {
        assertOrphanWarned(new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/abc/y", 2L)), "dump", 2);
    }

    public void testShardPrefixesSubPrefixBeyondShardCountWarns() throws Exception {
        assertOrphanWarned(new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/7/y", 2L)), "dump", 2);
    }

    public void testShardPrefixesNegativeSubPrefixWarns() throws Exception {
        assertOrphanWarned(new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/-1/y", 2L)), "dump", 2);
    }

    public void testShardPrefixesLeadingSlashSubPrefixWarns() throws Exception {
        assertOrphanWarned(new ObjectStoreManifest(Map.of("0/x", 1L, "/y", 2L)), "", 2);
    }

    public void testShardPrefixesZeroPaddedSubPrefixIsNotOwned() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("dump/0/x", 1L, "dump/00/y", 2L));
        assertThat(
            SliceAssignment.assign(config("shard_prefixes"), manifest, null, "dump", 0, 2),
            contains(new ObjectSlice("dump/0/x", 0, 1, 1))
        );
        assertOrphanWarned(manifest, "dump", 2);
    }

    private static ObjectStoreSourceConfig listConfig(Map<String, Object> extra) {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "shard_prefixes");
        params.put("discovery", "list");
        params.putAll(extra);
        return new ObjectStoreSourceConfig(params);
    }

    private void writeUnder(String path, String content) throws Exception {
        String[] segments = path.split("/");
        BlobPath blobPath = BlobPath.cleanPath();
        for (int i = 0; i < segments.length - 1; i++) {
            blobPath = blobPath.add(segments[i]);
        }
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            store.blobContainer(blobPath).writeBlob(segments[segments.length - 1], in, bytes.length, true);
        }
    }

    public void testListDiscoveryAssignsOnlyTheOwnedSubPrefix() throws Exception {
        writeUnder("0/a.ndjson", "a1\n");
        writeUnder("1/b.ndjson", "b1\nb2\n");
        ObjectStoreSourceConfig config = listConfig(Map.of());

        assertThat(
            SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 0, 2),
            contains(new ObjectSlice("0/a.ndjson", 0, 3, 3))
        );
        assertThat(
            SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 1, 2),
            contains(new ObjectSlice("1/b.ndjson", 0, 6, 6))
        );
    }

    public void testListDiscoveryIsDisjointAndCompleteAcrossShards() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        writeUnder("0/b.ndjson", "b\n");
        writeUnder("1/c.ndjson", "c\n");
        writeUnder("2/d.ndjson", "d\n");
        ObjectStoreSourceConfig config = listConfig(Map.of());

        Set<String> owned = new HashSet<>();
        for (int shard = 0; shard < 3; shard++) {
            for (ObjectSlice slice : SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", shard, 3)) {
                assertThat("owned twice: " + slice.getObjectName(), owned.add(slice.getObjectName()), is(true));
            }
        }
        assertThat(owned, is(Set.of("0/a.ndjson", "0/b.ndjson", "1/c.ndjson", "2/d.ndjson")));
    }

    public void testListDiscoveryWithBasePrefix() throws Exception {
        writeUnder("dump/0/a.ndjson", "a\n");
        ObjectStoreSourceConfig config = listConfig(Map.of("source_prefix", "dump/"));
        assertThat(
            SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "dump", 0, 2),
            contains(new ObjectSlice("dump/0/a.ndjson", 0, 2, 2))
        );
    }

    public void testListDiscoveryShardWithNoObjectsOwnsNothing() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        ObjectStoreSourceConfig config = listConfig(Map.of());
        assertThat(SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 1, 2), is(empty()));
    }

    public void testListDiscoveryMoreShardsThanSubPrefixes() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        ObjectStoreSourceConfig config = listConfig(Map.of());
        int owned = 0;
        for (int shard = 0; shard < 5; shard++) {
            owned += SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", shard, 5).size();
        }
        assertThat(owned, is(1));
    }

    public void testListDiscoveryIgnoresObjectsOutsideAnySubPrefix() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        writeUnder("orphan.ndjson", "o\n");
        writeUnder("abc/x.ndjson", "x\n");
        ObjectStoreSourceConfig config = listConfig(Map.of());
        assertThat(
            SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 0, 2),
            contains(new ObjectSlice("0/a.ndjson", 0, 2, 2))
        );
        assertThat(SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 1, 2), is(empty()));
    }

    public void testListDiscoveryEnforcesMaxObjects() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        writeUnder("0/b.ndjson", "b\n");
        ObjectStoreSourceConfig config = listConfig(Map.of("max_objects", 1L));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 0, 2)
        );
        assertThat(e.getMessage(), containsString("max_objects=1"));
    }

    public void testListDiscoveryEnforcesMaxListRequests() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        ObjectStoreSourceConfig config = listConfig(Map.of("max_list_requests", 1L));
        CostLimitExceededException e = expectThrows(
            CostLimitExceededException.class,
            () -> SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 0, 2)
        );
        assertThat(e.getMessage(), containsString("max_list_requests"));
    }

    public void testListDiscoveryZeroByteObjectYieldsEmptySlice() throws Exception {
        writeUnder("0/empty.ndjson", "");
        ObjectStoreSourceConfig config = listConfig(Map.of());
        assertThat(
            SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 0, 2),
            contains(new ObjectSlice("0/empty.ndjson", 0, 0, 0))
        );
    }

    public void testListDiscoveryLogsTheDiscoveredCountPerShard() throws Exception {
        writeUnder("0/a.ndjson", "a\n");
        ObjectStoreSourceConfig config = listConfig(Map.of());
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SliceAssignment.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "discovered count info",
                    SliceAssignment.class.getCanonicalName(),
                    Level.INFO,
                    "*shard 1 discovered 0 objects under sub-prefix [1/]*"
                )
            );
            SliceAssignment.assign(config, ObjectStoreManifest.EMPTY, container, "", 1, 2);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testShardPrefixesSummarisesMultipleUnownedObjectsInOneWarning() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(
            Map.of("dump/0/x", 1L, "dump/abc/a.ndjson", 2L, "dump/xyz/b.ndjson", 3L, "dump/orphan.ndjson", 4L)
        );
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SliceAssignment.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one summary warning naming the count and the first key",
                    SliceAssignment.class.getCanonicalName(),
                    Level.WARN,
                    "*[3] object(s), the first being [dump/abc/a.ndjson], are not under any shard sub-prefix*"
                )
            );
            SliceAssignment.assign(config("shard_prefixes"), manifest, null, "dump", 0, 2);
            appender.assertAllExpectationsMatched();
        }
    }
}
