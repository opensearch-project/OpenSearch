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
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.index.IngestionShardConsumer.ReadResult;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ObjectStoreShardConsumer} exercising the read loop against a real {@link FsBlobStore}.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ObjectStoreShardConsumerTests extends OpenSearchTestCase {

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

    private long write(String name, String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(name, in, bytes.length, true);
        }
        return bytes.length;
    }

    private static ObjectStoreSourceConfig config(String split) {
        return new ObjectStoreSourceConfig(Map.of("repository", "repo", "shard_split", split));
    }

    private static ObjectStoreSourceConfig config(String split, Map<String, Object> extra) {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", split);
        params.putAll(extra);
        return new ObjectStoreSourceConfig(params);
    }

    private ObjectStoreShardConsumer consumer(
        ObjectStoreSourceConfig config,
        BlobContainer readFrom,
        ObjectStoreManifest manifest,
        String resolvedPrefix,
        int shardId,
        int numShards
    ) throws IOException {
        return new ObjectStoreShardConsumer(
            config,
            readFrom,
            shardId,
            SliceAssignment.assign(config, manifest, readFrom, resolvedPrefix, shardId, numShards)
        );
    }

    private static List<String> payloads(List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results) {
        return results.stream().map(r -> new String(r.getMessage().getPayload(), StandardCharsets.UTF_8)).toList();
    }

    public void testReadsAllRecordsFromSingleObject() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(10, 1000);
        assertThat(payloads(results), contains("r1", "r2", "r3"));
        assertThat(results.get(0).getPointer().getByteOffset(), is(0L));
        assertThat(results.get(1).getPointer().getByteOffset(), is(3L));
        assertThat(results.get(2).getPointer().getByteOffset(), is(6L));
    }

    public void testUnboundedCostCapsWarnFromShardZero() throws Exception {
        long size = write("events.ndjson", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(ObjectStoreShardConsumer.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "unbounded caps warning",
                    ObjectStoreShardConsumer.class.getCanonicalName(),
                    Level.WARN,
                    "*cost caps are unbounded*"
                )
            );
            consumer(config("equal_bytes"), container, manifest, null, 0, 1);
            appender.assertAllExpectationsMatched();
        }

        Map<String, Object> bounded = new HashMap<>();
        bounded.put("max_bytes", 1000L);
        bounded.put("max_get_requests", 100L);
        bounded.put("max_list_requests", 100L);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(ObjectStoreShardConsumer.class))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no warning when caps bounded",
                    ObjectStoreShardConsumer.class.getCanonicalName(),
                    Level.WARN,
                    "*cost caps are unbounded*"
                )
            );
            consumer(config("equal_bytes", bounded), container, manifest, null, 0, 1);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLagReachesZeroWithTrailingBlankLine() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        assertThat(payloads(consumer.readNext(10, 1000)), contains("r1", "r2"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
    }

    public void testMaxBytesCountsActualBytesReadAcrossPolls() throws Exception {
        long size = write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        Map<String, Object> extra = new HashMap<>();
        extra.put("max_bytes", size * 3);
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes", extra), container, manifest, null, 0, 1);

        int nonEmptyPolls = 0;
        while (consumer.readNext(1, 1000).isEmpty() == false) {
            nonEmptyPolls++;
        }
        assertThat(nonEmptyPolls, is(4));
    }

    public void testContinuationAcrossReadNext() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        assertThat(payloads(consumer.readNext(2, 1000)), contains("r1", "r2"));
        assertThat(payloads(consumer.readNext(2, 1000)), contains("r3"));
        assertThat(consumer.readNext(2, 1000), is(empty()));
    }

    public void testReadFromPointerExcludingStart() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(
            new ObjectStorePointer("events.ndjson", 0),
            false,
            10,
            1000
        );
        assertThat(payloads(results), contains("r2", "r3"));
    }

    public void testReadFromPointerIncludingStart() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(
            new ObjectStorePointer("events.ndjson", 3),
            true,
            10,
            1000
        );
        assertThat(payloads(results), contains("r2", "r3"));
    }

    public void testEqualBytesTwoShardsPartitionRecords() throws Exception {
        long size = write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer shard0 = consumer(config("equal_bytes"), container, manifest, null, 0, 2);
        ObjectStoreShardConsumer shard1 = consumer(config("equal_bytes"), container, manifest, null, 1, 2);

        assertThat(payloads(shard0.readNext(10, 1000)), contains("aaaa", "bbbb", "cccc"));
        assertThat(payloads(shard1.readNext(10, 1000)), contains("dddd"));
    }

    public void testWholeFilesReadsAcrossObjects() throws Exception {
        long a = write("a", "a1\na2\n");
        long b = write("b", "b1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);

        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(10, 1000);
        assertThat(payloads(results), contains("a1", "a2", "b1"));
        assertThat(results.get(2).getPointer().getObjectKey(), is("b"));
        assertThat(results.get(2).getPointer().getByteOffset(), is(0L));
    }

    public void testEmptyShardReturnsNothingAndEqualPointers() throws Exception {
        long size = write("small", "aa\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("small", size));
        ObjectStoreShardConsumer emptyShard = consumer(config("equal_bytes"), container, manifest, null, 1, 3);

        assertThat(emptyShard.readNext(10, 1000), is(empty()));
        assertThat(emptyShard.earliestPointer(), is(emptyShard.latestPointer()));
    }

    public void testEarliestAndLatestPointers() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        ObjectStorePointer earliest = (ObjectStorePointer) consumer.earliestPointer();
        ObjectStorePointer latest = (ObjectStorePointer) consumer.latestPointer();
        assertThat(earliest.getByteOffset(), is(0L));
        assertThat(latest.getByteOffset(), is(size));
        assertThat(consumer.getShardId(), is(0));
    }

    public void testPointerBasedLagFromEarliestEqualsTotalBytes() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(size));
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("events.ndjson", 6)), is(size - 6));
    }

    public void testPointerFromOffsetRoundTrips() throws Exception {
        long size = write("events.ndjson", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);

        ObjectStorePointer original = new ObjectStorePointer("events.ndjson", 3);
        assertThat(consumer.pointerFromOffset(original.asString()), is(original));
    }

    public void testTimestampPointerUnsupported() throws Exception {
        long size = write("events.ndjson", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        expectThrows(UnsupportedOperationException.class, () -> consumer.pointerFromTimestampMillis(1L));
    }

    public void testMaxMessagesZeroReturnsEmpty() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(consumer.readNext(0, 1000), is(empty()));
    }

    public void testNegativeMaxMessagesReadsAll() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(-1, 1000)), contains("r1", "r2", "r3"));
    }

    public void testReadFromPointerForUnownedObjectThrows() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        PointerNotOwnedException e = expectThrows(
            PointerNotOwnedException.class,
            () -> consumer.readNext(new ObjectStorePointer("not-owned", 0), true, 10, 1000)
        );
        assertThat(e.getMessage(), containsString("not-owned"));
        assertThat(e.getMessage(), containsString("not among the 1 slices owned by shard 0"));
    }

    public void testUnownedPointerMessageNamesTheLikelyLayoutChangeAndTheRemedy() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        PointerNotOwnedException e = expectThrows(
            PointerNotOwnedException.class,
            () -> consumer.readNext(new ObjectStorePointer("old-layout.ndjson", 3), true, 10, 1000)
        );
        assertThat(e.getMessage(), containsString("refers to object [old-layout.ndjson]"));
        assertThat(e.getMessage(), containsString("source layout or manifest changed after ingestion started"));
        assertThat(e.getMessage(), containsString("[manifest], [source_url], [source_prefix], [shard_split] or [discovery] param"));
        assertThat(e.getMessage(), containsString("which is not supported"));
        assertThat(e.getMessage(), containsString("recreate the index"));
    }

    public void testReadFromPointerForOwnedObjectStillWorks() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(new ObjectStorePointer("events.ndjson", 3), true, 10, 1000)), contains("r2"));
    }

    public void testLimitEnforcedAcrossObjects() throws Exception {
        long a = write("a", "a1\na2\n");
        long b = write("b", "b1\nb2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(3, 1000)), contains("a1", "a2", "b1"));
    }

    public void testPointerBasedLagWithNonObjectStorePointerUsesTotal() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(consumer.getPointerBasedLag(null), is(size));
    }

    public void testPointerBasedLagForUnownedPointerCountsFromStart() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("not-owned", 0)), is(size));
    }

    public void testPointerBasedLagForUnownedPointerIgnoresItsOffset() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("not-owned", size)), is(size));
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("not-owned", Long.MAX_VALUE - 1)), is(size));
    }

    public void testEmptyShardPointerBasedLagIsZero() throws Exception {
        long size = write("small", "aa\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("small", size));
        ObjectStoreShardConsumer emptyShard = consumer(config("equal_bytes"), container, manifest, null, 1, 3);
        assertThat(emptyShard.getPointerBasedLag(emptyShard.earliestPointer()), is(0L));
    }

    public void testCloseIsNoOpAndReadsStillSucceedAfterIt() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        consumer.close();
        assertThat(payloads(consumer.readNext(10, 1000)), contains("r1", "r2"));
    }

    public void testReadNextWrapsIOException() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        BlobContainer failing = mock(BlobContainer.class);
        when(failing.readBlob(anyString(), anyLong(), anyLong())).thenThrow(new IOException("boom"));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), failing, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getMessage(), containsString("failed to read from object storage"));
    }

    public void testReadNextMessageNamesTheUnderlyingCause() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        BlobContainer failing = mock(BlobContainer.class);
        when(failing.readBlob(anyString(), anyLong(), anyLong())).thenThrow(new IOException("the underlying reason"));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), failing, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getMessage(), containsString("the underlying reason"));
    }

    public void testSliceResolutionFailurePropagates() throws Exception {
        BlobContainer failing = mock(BlobContainer.class);
        when(failing.readBlob(anyString(), anyLong(), anyLong())).thenThrow(new IOException("read failed"));
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 100L));
        IOException e = expectThrows(IOException.class, () -> consumer(config("equal_bytes"), failing, manifest, null, 0, 2));
        assertThat(e.getMessage(), is("read failed"));
    }

    public void testSkipsExhaustedSliceAndContinues() throws Exception {
        long a = write("a", "a1\n");
        long b = write("b", "b1\nb2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(
            new ObjectStorePointer("a", a),
            true,
            10,
            1000
        );
        assertThat(payloads(results), contains("b1", "b2"));
    }

    public void testLagFromMidPointerAcrossObjects() throws Exception {
        long a = write("a", "a1\na2\n");
        long b = write("b", "b1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("a", 3)), is(a - 3 + b));
    }

    public void testLagCountsZeroWhenPointerBeyondEarlierObjectEnd() throws Exception {
        long a = write("a", "a1\n");
        long b = write("b", "b1\nb2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("a", a)), is(b));
    }

    public void testNullManifestRejected() throws Exception {
        expectThrows(NullPointerException.class, () -> consumer(config("equal_bytes"), container, null, null, 0, 1));
    }

    public void testMaxGetRequestsExceededThrows() throws Exception {
        long a = write("a", "a1\na2\n");
        long b = write("b", "b1\nb2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_get_requests", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause(), is(instanceOf(CostLimitExceededException.class)));
        assertThat(e.getCause().getMessage(), containsString("max_get_requests"));
    }

    public void testMaxBytesExceededThrows() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreSourceConfig cfg = config("equal_bytes", Map.of("max_bytes", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause(), is(instanceOf(CostLimitExceededException.class)));
        assertThat(e.getCause().getMessage(), containsString("max_bytes"));
    }

    public void testGenerousCostCapsReadNormally() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreSourceConfig cfg = config(
            "equal_bytes",
            Map.of("max_get_requests", 100L, "max_bytes", 1_000_000L, "max_list_requests", 100L)
        );
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("r1", "r2", "r3"));
    }

    public void testWriteOnceTruncatedObjectThrows() throws Exception {
        write("a", "a1\na2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 100L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("truncated"));
        assertThat(e.getCause().getMessage(), containsString("truncated"));
    }

    public void testWriteOnceManifestLargerThanActualDetectedAsTruncation() throws Exception {
        write("a", "a1\na2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", 20L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("truncated"));
    }

    public void testWriteOnceMatchingSizeReadsNormally() throws Exception {
        long a = write("a", "a1\na2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("a1", "a2"));
    }

    public void testMissingObjectSurfacesAsReadError() throws Exception {
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("missing", 10L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
    }

    public void testReadUnderLimitDoesNotTriggerTruncationCheck() throws Exception {
        long a = write("a", "a1\na2\na3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(1, 1000)), contains("a1"));
    }

    public void testLimitReachedWithinSingleFrameStopsAtLimit() throws Exception {
        long size = write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\neeee\nffff\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer shard1 = consumer(config("equal_bytes"), container, manifest, null, 1, 2);

        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = shard1.readNext(
            new ObjectStorePointer("events.ndjson", 10),
            false,
            1,
            1000
        );
        assertThat(payloads(results), contains("eeee"));
    }

    public void testLimitReachedAcrossSlicesStopsLoop() throws Exception {
        long a = write("a", "a1\n");
        long b = write("b", "b1\n");
        long c = write("c", "c1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b, "c", c));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(2, 1000)).size(), is(2));
    }

    public void testCompressedObjectRejected() throws Exception {
        byte[] gzip = new byte[] { (byte) 0x1f, (byte) 0x8b, (byte) 'x', (byte) '\n' };
        try (InputStream in = new ByteArrayInputStream(gzip)) {
            container.writeBlob("g.ndjson", in, gzip.length, true);
        }
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("g.ndjson", (long) gzip.length));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("gzip-compressed"));
    }

    public void testFirstByteGzipMagicButSecondByteDiffersIsNotRejected() throws Exception {
        byte[] bytes = new byte[] { (byte) 0x1f, (byte) 'x', (byte) '\n' };
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob("nearly.ndjson", in, bytes.length, true);
        }
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("nearly.ndjson", (long) bytes.length));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(10, 1000);
        assertThat(results.size(), is(1));
        assertThat(results.get(0).getMessage().getPayload().length, is(2));
    }

    public void testSubTwoByteObjectSkipsCompressionCheckAndReads() throws Exception {
        long size = write("tiny", "x");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("tiny", size));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("x"));
    }

    public void testMaxValueDeclaredSizeFailsAsTruncationNotAsIllegalArgument() throws Exception {
        write("huge.ndjson", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("huge.ndjson", Long.MAX_VALUE));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("the object was truncated"));
    }

    private void writeGzipMagic(BlobContainer target, String name) throws Exception {
        byte[] gzip = new byte[] { (byte) 0x1f, (byte) 0x8b, (byte) 'x', (byte) '\n', (byte) 'y', (byte) '\n' };
        try (InputStream in = new ByteArrayInputStream(gzip)) {
            target.writeBlob(name, in, gzip.length, true);
        }
    }

    public void testCompressedObjectRejectedUnderWholeFiles() throws Exception {
        writeGzipMagic(container, "g.ndjson");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("g.ndjson", 6L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("gzip-compressed"));
    }

    public void testCompressedObjectRejectedUnderShardPrefixes() throws Exception {
        writeGzipMagic(store.blobContainer(BlobPath.cleanPath().add("p").add("0")), "g.ndjson");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("p/0/g.ndjson", 6L));
        ObjectStoreShardConsumer consumer = consumer(config("shard_prefixes"), container, manifest, "p", 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("gzip-compressed"));
    }

    public void testCompressedObjectRejectedForNonZeroOffsetSlice() throws Exception {
        writeGzipMagic(container, "g.ndjson");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("g.ndjson", 6L));
        ObjectStoreShardConsumer shardOne = consumer(config("equal_bytes"), container, manifest, null, 1, 2);
        assertThat(shardOne.earliestPointer(), is(new ObjectStorePointer("g.ndjson", 4L)));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> shardOne.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("gzip-compressed"));
    }

    public void testCompressedObjectRejectedOnResumeFromNonZeroOffset() throws Exception {
        writeGzipMagic(container, "g.ndjson");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("g.ndjson", 6L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(
            UncheckedIOException.class,
            () -> consumer.readNext(new ObjectStorePointer("g.ndjson", 4), true, 10, 1000)
        );
        assertThat(e.getCause().getMessage(), containsString("gzip-compressed"));
    }

    public void testCompressionCheckFromOffsetZeroCostsNoExtraGetRequest() throws Exception {
        long size = write("a", "a1\na2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", size));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_get_requests", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("a1", "a2"));
    }

    public void testCompressionProbeCountsOnlyTheBytesDelivered() throws Exception {
        write("short", "x");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("short", 2L));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_bytes", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(
            UncheckedIOException.class,
            () -> consumer.readNext(new ObjectStorePointer("short", 1), true, 10, 1000)
        );
        assertThat(e.getCause().getMessage(), containsString("truncated"));
    }

    public void testLagReachesZeroAfterFullConsumption() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(size));
        assertThat(consumer.readNext(10, 1000).size(), is(3));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
    }

    public void testLagDecreasesAfterPartialConsumption() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(consumer.readNext(1, 1000).size(), is(1));
        long lagAfterOne = consumer.getPointerBasedLag(consumer.earliestPointer());
        assertThat(lagAfterOne, is(size - 3));
    }

    public void testLagReachesZeroAcrossObjects() throws Exception {
        long a = write("a", "a1\na2\n");
        long b = write("b", "b1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(consumer.readNext(10, 1000).size(), is(3));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
    }

    public void testWarnsWhenOnlyMaxGetRequestsIsUnbounded() throws Exception {
        long size = write("events.ndjson", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        Map<String, Object> params = new HashMap<>();
        params.put("max_bytes", 1000L);
        params.put("max_list_requests", 100L);
        assertUnboundedCapWarning(manifest, params);
    }

    public void testWarnsWhenOnlyMaxListRequestsIsUnbounded() throws Exception {
        long size = write("events.ndjson", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        Map<String, Object> params = new HashMap<>();
        params.put("max_bytes", 1000L);
        params.put("max_get_requests", 100L);
        assertUnboundedCapWarning(manifest, params);
    }

    private void assertUnboundedCapWarning(ObjectStoreManifest manifest, Map<String, Object> params) throws Exception {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(ObjectStoreShardConsumer.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "unbounded caps warning",
                    ObjectStoreShardConsumer.class.getCanonicalName(),
                    Level.WARN,
                    "*cost caps are unbounded*"
                )
            );
            consumer(config("equal_bytes", params), container, manifest, null, 0, 1);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testCompressionProbeDeliveringNoBytesIsNotBilled() throws Exception {
        BlobContainer empty = mock(BlobContainer.class);
        when(empty.readBlob(anyString(), anyLong(), anyLong())).thenAnswer(invocation -> new ByteArrayInputStream(new byte[0]));
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("e", 10L));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_bytes", 0L));
        ObjectStoreShardConsumer consumer = consumer(cfg, empty, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(
            UncheckedIOException.class,
            () -> consumer.readNext(new ObjectStorePointer("e", 1), true, 10, 1000)
        );
        assertThat(e.getCause().getMessage(), containsString("truncated"));
    }

    public void testListDiscoveryReadsRecordsWithoutAManifest() throws Exception {
        BlobPath shardPath = BlobPath.cleanPath().add("0");
        byte[] bytes = "r1\nr2\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            store.blobContainer(shardPath).writeBlob("part.ndjson", in, bytes.length, true);
        }
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "shard_prefixes");
        params.put("discovery", "list");
        ObjectStoreShardConsumer consumer = consumer(new ObjectStoreSourceConfig(params), container, ObjectStoreManifest.EMPTY, "", 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("r1", "r2"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
    }

    public void testListDiscoveryDetectsATruncatedObject() throws Exception {
        BlobContainer shardContainer = store.blobContainer(BlobPath.cleanPath().add("0"));
        byte[] bytes = "r1\nr2\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            shardContainer.writeBlob("part.ndjson", in, bytes.length, true);
        }
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "shard_prefixes");
        params.put("discovery", "list");
        ObjectStoreShardConsumer consumer = consumer(new ObjectStoreSourceConfig(params), container, ObjectStoreManifest.EMPTY, "", 0, 1);
        byte[] shorter = "r1\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(shorter)) {
            shardContainer.writeBlob("part.ndjson", in, shorter.length, false);
        }
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("truncated"));
    }

    public void testExhaustedShardIssuesNoFurtherGetRequests() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreSourceConfig cfg = config("equal_bytes", Map.of("max_get_requests", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("r1", "r2"));
        for (int poll = 0; poll < 20; poll++) {
            assertThat(consumer.readNext(10, 1000), is(empty()));
        }
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
    }

    public void testResumeAcrossPollsReadsEachRecordExactlyOnce() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\nr4\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        List<String> seen = new ArrayList<>();
        for (int poll = 0; poll < 6; poll++) {
            seen.addAll(payloads(consumer.readNext(1, 1000)));
        }
        assertThat(seen, contains("r1", "r2", "r3", "r4"));
    }

    private void overwrite(String name, byte[] bytes) throws Exception {
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(name, in, bytes.length, false);
        }
    }

    public void testFailedPollDoesNotAdvanceResumeOffsetPastDiscardedRecords() throws Exception {
        long a = write("a", "a1\na2\n");
        byte[] gzip = new byte[] { (byte) 0x1f, (byte) 0x8b, (byte) 'x', (byte) '\n' };
        overwrite("b", gzip);
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", (long) gzip.length));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);

        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("gzip-compressed"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(a + gzip.length));

        overwrite("b", "bb1\n".getBytes(StandardCharsets.UTF_8));
        assertThat(payloads(consumer.readNext(10, 1000)), contains("a1", "a2", "bb1"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
    }

    public void testFailedPollKeepsTheResumeOffsetOfTheLastSuccessfulPoll() throws Exception {
        long a = write("a", "a1\na2\n");
        byte[] gzip = new byte[] { (byte) 0x1f, (byte) 0x8b, (byte) 'x', (byte) '\n' };
        overwrite("b", gzip);
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", (long) gzip.length));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);

        assertThat(payloads(consumer.readNext(1, 1000)), contains("a1"));
        expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(a - 3 + gzip.length));

        overwrite("b", "bb1\n".getBytes(StandardCharsets.UTF_8));
        assertThat(payloads(consumer.readNext(10, 1000)), contains("a2", "bb1"));
    }

    public void testTimeoutStopsBeforeTheNextSliceAndKeepsWhatWasRead() throws Exception {
        long a = write("a", "a1\n");
        long b = write("b", "b1\n");
        long c = write("c", "c1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a, "b", b, "c", c));
        ObjectStoreSourceConfig cfg = config("whole_files");
        List<ObjectSlice> slices = SliceAssignment.assign(cfg, manifest, container, null, 0, 1);
        ObjectStoreShardConsumer consumer = new ObjectStoreShardConsumer(cfg, container, 0, slices) {
            private int calls;

            @Override
            long nanoTime() {
                return calls++ <= 1 ? 0L : Long.MAX_VALUE / 2;
            }
        };
        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = consumer.readNext(10, 1000);
        assertThat(payloads(results), contains("a1"));
    }

    public void testTimeoutBeforeTheFirstSliceReturnsNothing() throws Exception {
        long a = write("a", "a1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a", a));
        ObjectStoreSourceConfig cfg = config("whole_files");
        List<ObjectSlice> slices = SliceAssignment.assign(cfg, manifest, container, null, 0, 1);
        ObjectStoreShardConsumer consumer = new ObjectStoreShardConsumer(cfg, container, 0, slices) {
            private int calls;

            @Override
            long nanoTime() {
                return calls++ == 0 ? 0L : Long.MAX_VALUE / 2;
            }
        };
        assertThat(consumer.readNext(10, 1000), is(empty()));
    }

    public void testNonPositiveTimeoutDisablesTheDeadline() throws Exception {
        long size = write("events.ndjson", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(new ObjectStorePointer("events.ndjson", 0), true, 10, 0)), contains("r1", "r2"));
    }

    public void testForcedPointerAtTheEndIsRememberedSoTheNextPollDoesNotRestart() throws Exception {
        long size = write("events.ndjson", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreShardConsumer consumer = consumer(config("equal_bytes"), container, manifest, null, 0, 1);
        ObjectStorePointer end = (ObjectStorePointer) consumer.latestPointer();

        assertThat(consumer.readNext(end, true, 10, 1000), is(empty()));
        assertThat("a forced pointer at the end must not be forgotten", payloads(consumer.readNext(10, 1000)), is(empty()));
    }

    public void testMaxBytesStopsTheReadInFlightInsteadOfAfterTheWholeSlice() throws Exception {
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 4000; i++) {
            big.append("{\"n\":").append(i).append("}\n");
        }
        long size = write("big.ndjson", big.toString());
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("big.ndjson", size));
        AtomicLong delivered = new AtomicLong();
        BlobContainer counting = new FilterBlobContainer(container) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                return new FilterInputStream(super.readBlob(name, position, length)) {
                    @Override
                    public int read() throws IOException {
                        int b = super.read();
                        if (b != -1) {
                            delivered.incrementAndGet();
                        }
                        return b;
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        int n = super.read(b, off, len);
                        if (n > 0) {
                            delivered.addAndGet(n);
                        }
                        return n;
                    }
                };
            }
        };
        ObjectStoreSourceConfig cfg = config("equal_bytes", Map.of("max_bytes", 100L));
        ObjectStoreShardConsumer consumer = consumer(cfg, counting, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(-1, 1000));
        assertThat(e.getCause(), is(instanceOf(CostLimitExceededException.class)));
        assertThat("the cap must abandon the transfer, not download the whole slice", delivered.get(), lessThan(size / 4));
    }

    public void testCountingInputStreamCountsBothReadPaths() throws Exception {
        byte[] data = "abcdef".getBytes(StandardCharsets.UTF_8);
        try (
            ObjectStoreShardConsumer.CountingInputStream counting = new ObjectStoreShardConsumer.CountingInputStream(
                new ByteArrayInputStream(data),
                Long.MAX_VALUE,
                Long.MAX_VALUE
            )
        ) {
            assertThat(counting.count(), is(0L));
            assertThat(counting.read(), is((int) 'a'));
            assertThat(counting.count(), is(1L));
            byte[] bulk = new byte[3];
            assertThat(counting.read(bulk, 0, 3), is(3));
            assertThat(counting.count(), is(4L));
            assertThat(counting.read(new byte[8], 0, 8), is(2));
            assertThat(counting.count(), is(6L));
            assertThat(counting.read(), is(-1));
            assertThat(counting.read(new byte[4], 0, 4), is(-1));
            assertThat(counting.count(), is(6L));
        }
    }

    public void testCountingInputStreamEndsAtItsLimitOnBothReadPaths() throws Exception {
        ByteArrayInputStream delegate = new ByteArrayInputStream(new byte[] { 'a', 'b', 'c', (byte) 0xff, 'e', 'f' });
        try (
            ObjectStoreShardConsumer.CountingInputStream counting = new ObjectStoreShardConsumer.CountingInputStream(
                delegate,
                4,
                Long.MAX_VALUE
            )
        ) {
            byte[] bulk = new byte[8];
            assertThat(counting.read(bulk, 0, 8), is(4));
            assertThat(counting.count(), is(4L));
            assertThat(counting.read(bulk, 0, 8), is(-1));
            assertThat(counting.read(), is(-1));
            assertThat(counting.count(), is(4L));
            assertThat(delegate.read(), is((int) 'e'));
        }
        try (
            ObjectStoreShardConsumer.CountingInputStream counting = new ObjectStoreShardConsumer.CountingInputStream(
                new ByteArrayInputStream(new byte[] { 'a', 'b' }),
                1,
                Long.MAX_VALUE
            )
        ) {
            assertThat(counting.read(), is((int) 'a'));
            assertThat(counting.read(), is(-1));
            assertThat(counting.count(), is(1L));
        }
    }

    public void testObjectLargerThanManifestSizeFailsWithoutEmittingTheCutRecord() throws Exception {
        write("o", "{\"a\":1}\n{\"b\":2}\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 12L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("object [o] has bytes beyond offset [12]"));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(12L));
    }

    public void testObjectLargerThanManifestSizeEndingAtALineEndFails() throws Exception {
        write("o", "{\"a\":1}\n{\"b\":2}\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 8L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testObjectLargerThanManifestSizeFailsOnThePollThatReachesTheDeclaredEnd() throws Exception {
        write("o", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 6L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(1, 1000)), contains("r1"));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(3L));
    }

    public void testObjectLargerThanManifestSizeFailsWhenThePollLimitEndsAtTheDeclaredEnd() throws Exception {
        write("o", "r1\nr2\nr3\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 6L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(2, 1000));
        assertThat(e.getCause().getMessage(), containsString("object [o] has bytes beyond offset [6]"));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(6L));
        UncheckedIOException again = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(again.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testExactManifestSizeWithThePollLimitEndingAtTheDeclaredEndSucceeds() throws Exception {
        long size = write("o", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", size));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(2, 1000)), contains("r1", "r2"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
        assertThat(consumer.readNext(10, 1000), is(empty()));
    }

    public void testEqualBytesLargerThanManifestSizeFailsWithBatchSizeOne() throws Exception {
        write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", 15L));
        ObjectStoreShardConsumer shard0 = consumer(config("equal_bytes"), container, manifest, null, 0, 2);
        ObjectStoreShardConsumer shard1 = consumer(config("equal_bytes"), container, manifest, null, 1, 2);
        assertThat(shard1.earliestPointer(), is(new ObjectStorePointer("events.ndjson", 10L)));
        assertThat(payloads(shard0.readNext(1, 1000)), contains("aaaa"));
        assertThat(payloads(shard0.readNext(1, 1000)), contains("bbbb"));
        assertThat(shard0.readNext(1, 1000), is(empty()));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> shard1.readNext(1, 1000));
        assertThat(e.getCause().getMessage(), containsString("object [events.ndjson] has bytes beyond offset [15]"));
        assertThat(shard1.getPointerBasedLag(shard1.earliestPointer()), is(5L));
        UncheckedIOException again = expectThrows(UncheckedIOException.class, () -> shard1.readNext(1, 1000));
        assertThat(again.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testNonEmptyObjectDeclaredEmptyFails() throws Exception {
        write("o", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 0L));
        ObjectStoreShardConsumer consumer = consumer(config("whole_files"), container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("object [o] has bytes beyond offset [0]"));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
        UncheckedIOException again = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(again.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testNonEmptyObjectDeclaredEmptyFailsBeforeLaterObjectsAreReturned() throws Exception {
        write("a", "r1\n");
        write("b", "b1\n");
        ObjectStoreShardConsumer consumer = new ObjectStoreShardConsumer(
            config("whole_files"),
            container,
            0,
            List.of(new ObjectSlice("a", 0, 0, 0), new ObjectSlice("b", 0, 3, 3))
        );
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("object [a] has bytes beyond offset [0]"));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(3L));
    }

    public void testNonEmptyObjectDeclaredEmptyIsNotMaskedByTheBytesCap() throws Exception {
        write("o", "r1\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 0L));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_get_requests", 1L, "max_bytes", 0L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause(), is(not(instanceOf(CostLimitExceededException.class))));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testEmptyObjectDeclaredEmptySucceedsWithOneGetRequest() throws Exception {
        write("empty", "");
        write("b", "b1\n");
        ObjectStoreSourceConfig cfg = config("whole_files");
        List<ObjectSlice> slices = List.of(new ObjectSlice("b", 0, 3, 3), new ObjectSlice("empty", 0, 0, 0));
        BlobContainer recording = mock(BlobContainer.class);
        when(recording.readBlob(anyString())).thenAnswer(invocation -> container.readBlob(invocation.<String>getArgument(0)));
        when(recording.readBlob(anyString(), anyLong(), anyLong())).thenAnswer(
            invocation -> container.readBlob(
                invocation.<String>getArgument(0),
                invocation.<Long>getArgument(1),
                invocation.<Long>getArgument(2)
            )
        );
        ObjectStoreShardConsumer consumer = new ObjectStoreShardConsumer(cfg, recording, 0, slices);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("b1"));
        assertThat(consumer.readNext(10, 1000), is(empty()));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(0L));
        verify(recording).readBlob("b", 0L, 4L);
        verify(recording).readBlob("empty");
        verifyNoMoreInteractions(recording);
    }

    public void testEmptyObjectDeclaredEmptyCountsAgainstTheGetRequestCap() throws Exception {
        write("e1", "");
        write("e2", "");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("e1", 0L, "e2", 0L));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_get_requests", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause(), instanceOf(CostLimitExceededException.class));
        assertThat(e.getCause().getMessage(), containsString("max_get_requests [1]"));
    }

    public void testObjectLargerThanManifestSizeIsDetectedWithoutAnExtraGetRequest() throws Exception {
        write("o", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", 3L));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_get_requests", 1L));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause(), is(not(instanceOf(CostLimitExceededException.class))));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testExactManifestSizeIsNotBilledForTheByteBeyondTheEnd() throws Exception {
        long size = write("o", "r1\nr2\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("o", size));
        ObjectStoreSourceConfig cfg = config("whole_files", Map.of("max_get_requests", 1L, "max_bytes", size));
        ObjectStoreShardConsumer consumer = consumer(cfg, container, manifest, null, 0, 1);
        assertThat(payloads(consumer.readNext(10, 1000)), contains("r1", "r2"));
        assertThat(consumer.readNext(10, 1000), is(empty()));
    }

    public void testEqualBytesLastSliceLargerThanManifestSizeFails() throws Exception {
        write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", 17L));
        ObjectStoreShardConsumer shard1 = consumer(config("equal_bytes"), container, manifest, null, 1, 2);
        assertThat(shard1.earliestPointer(), is(new ObjectStorePointer("events.ndjson", 10L)));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> shard1.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testEqualBytesEarlierSliceStillStopsAtItsEndWithoutReadingBeyond() throws Exception {
        long size = write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreSourceConfig cfg = config("equal_bytes", Map.of("max_get_requests", 1L, "max_bytes", 15L));
        ObjectStoreShardConsumer shard0 = consumer(cfg, container, manifest, null, 0, 2);
        assertThat(shard0.latestPointer(), is(new ObjectStorePointer("events.ndjson", 15L)));
        assertThat(payloads(shard0.readNext(10, 1000)), contains("aaaa", "bbbb", "cccc"));
        assertThat(shard0.getPointerBasedLag(shard0.earliestPointer()), is(0L));
    }

    public void testEqualBytesLastSliceEndingOnANewlineLargerThanManifestSizeFails() throws Exception {
        write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", 15L));
        ObjectStoreShardConsumer shard0 = consumer(config("equal_bytes"), container, manifest, null, 0, 2);
        ObjectStoreShardConsumer shard1 = consumer(config("equal_bytes"), container, manifest, null, 1, 2);
        assertThat(shard1.earliestPointer(), is(new ObjectStorePointer("events.ndjson", 10L)));
        assertThat(shard1.latestPointer(), is(new ObjectStorePointer("events.ndjson", 15L)));
        assertThat(payloads(shard0.readNext(10, 1000)), contains("aaaa", "bbbb"));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> shard1.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("object [events.ndjson] has bytes beyond offset [15]"));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
        assertThat(shard1.getPointerBasedLag(shard1.earliestPointer()), is(5L));
    }

    public void testEqualBytesEarlierSliceReachingTheDeclaredEndOnANewlineLargerThanManifestSizeFails() throws Exception {
        write("events.ndjson", "aaaa\nbbbbbbbb\ncc\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", 14L));
        ObjectStoreShardConsumer shard0 = consumer(config("equal_bytes"), container, manifest, null, 0, 2);
        ObjectStoreShardConsumer shard1 = consumer(config("equal_bytes"), container, manifest, null, 1, 2);
        assertThat(shard0.latestPointer(), is(new ObjectStorePointer("events.ndjson", 14L)));
        assertThat(shard1.readNext(10, 1000), is(empty()));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> shard0.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
    }

    public void testEqualBytesEarlierSliceRequestsExactlyItsRange() throws Exception {
        long size = write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreSourceConfig cfg = config("equal_bytes");
        List<ObjectSlice> slices = SliceAssignment.assign(cfg, manifest, container, null, 0, 2);
        BlobContainer recording = mock(BlobContainer.class);
        when(recording.readBlob(anyString(), anyLong(), anyLong())).thenAnswer(
            invocation -> container.readBlob(
                invocation.<String>getArgument(0),
                invocation.<Long>getArgument(1),
                invocation.<Long>getArgument(2)
            )
        );
        ObjectStoreShardConsumer shard0 = new ObjectStoreShardConsumer(cfg, recording, 0, slices);
        assertThat(payloads(shard0.readNext(10, 1000)), contains("aaaa", "bbbb", "cccc"));
        verify(recording).readBlob("events.ndjson", 0L, 15L);
        verifyNoMoreInteractions(recording);
    }

    public void testEqualBytesLastSliceRequestsOneByteBeyondItsRange() throws Exception {
        long size = write("events.ndjson", "aaaa\nbbbb\ncccc\ndddd\n");
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("events.ndjson", size));
        ObjectStoreSourceConfig cfg = config("equal_bytes");
        List<ObjectSlice> slices = SliceAssignment.assign(cfg, manifest, container, null, 1, 2);
        BlobContainer recording = mock(BlobContainer.class);
        when(recording.readBlob(anyString(), anyLong(), anyLong())).thenAnswer(
            invocation -> container.readBlob(
                invocation.<String>getArgument(0),
                invocation.<Long>getArgument(1),
                invocation.<Long>getArgument(2)
            )
        );
        ObjectStoreShardConsumer shard1 = new ObjectStoreShardConsumer(cfg, recording, 1, slices);
        assertThat(payloads(shard1.readNext(10, 1000)), contains("dddd"));
        verify(recording).readBlob("events.ndjson", 0L, 2L);
        verify(recording).readBlob("events.ndjson", 15L, 6L);
        verifyNoMoreInteractions(recording);
    }

    public void testListDiscoveryDetectsAnObjectThatGrewAfterListing() throws Exception {
        BlobContainer shardContainer = store.blobContainer(BlobPath.cleanPath().add("0"));
        byte[] bytes = "r1\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            shardContainer.writeBlob("part.ndjson", in, bytes.length, true);
        }
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "shard_prefixes");
        params.put("discovery", "list");
        ObjectStoreShardConsumer consumer = consumer(new ObjectStoreSourceConfig(params), container, ObjectStoreManifest.EMPTY, "", 0, 1);
        byte[] longer = "r1\nr2\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(longer)) {
            shardContainer.writeBlob("part.ndjson", in, longer.length, false);
        }
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause().getMessage(), containsString("larger than its manifest size"));
    }
}
