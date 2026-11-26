/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.IngestionShardConsumer.ReadResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class SliceAssignmentLineBoundaryTests extends OpenSearchTestCase {

    private static final long MAX_RECORD_BYTES = 4;
    private static final String NAME = "o";
    private static final String BOM = "﻿";

    private BlobContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Path dataDir = createTempDir().resolve("data");
        Files.createDirectories(dataDir);
        container = new FsBlobStore(8192, dataDir, false).blobContainer(BlobPath.cleanPath());
    }

    private long write(String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(NAME, in, bytes.length, false);
        }
        return bytes.length;
    }

    private static ObjectStoreSourceConfig config(String split) {
        return new ObjectStoreSourceConfig(Map.of("repository", "repo", "shard_split", split, "max_record_bytes", MAX_RECORD_BYTES));
    }

    private static ObjectStoreManifest manifest(long size) {
        return new ObjectStoreManifest(Map.of(NAME, size));
    }

    private List<ObjectSlice> assign(long size, int shardId, int numShards) throws IOException {
        return SliceAssignment.assign(config("equal_bytes"), manifest(size), container, null, shardId, numShards);
    }

    private List<String> read(String split, List<ObjectSlice> slices) throws Exception {
        ObjectStoreShardConsumer consumer = new ObjectStoreShardConsumer(config(split), container, 0, slices);
        List<String> payloads = new ArrayList<>();
        for (ReadResult<ObjectStorePointer, ObjectStoreMessage> result : consumer.readNext(100, 1000)) {
            payloads.add(new String(result.getMessage().getPayload(), StandardCharsets.UTF_8));
        }
        return payloads;
    }

    private List<String> readWholeFile(long size) throws Exception {
        return read("whole_files", SliceAssignment.assign(config("whole_files"), manifest(size), container, null, 0, 1));
    }

    private void assertDisjointCompleteAndEquivalent(String content) throws Exception {
        long size = write(content);
        List<String> expected = readWholeFile(size);
        for (int numShards = 1; numShards <= size + 1; numShards++) {
            List<ObjectSlice> all = new ArrayList<>();
            for (int shard = 0; shard < numShards; shard++) {
                all.addAll(assign(size, shard, numShards));
            }
            all.sort(Comparator.comparingLong(ObjectSlice::getStartOffset));
            long next = 0;
            List<String> records = new ArrayList<>();
            for (ObjectSlice slice : all) {
                assertThat("gap or overlap at shards=" + numShards, slice.getStartOffset(), is(next));
                next = slice.getEndOffsetExclusive();
                records.addAll(read("equal_bytes", List.of(slice)));
            }
            assertThat("slices do not cover the object at shards=" + numShards, next, is(size));
            assertThat("records differ from whole_files at shards=" + numShards, records, equalTo(expected));
        }
    }

    public void testCrlfLineOfExactlyMaxRecordBytesStartingAtTheCutIsOwnedByThePreviousShard() throws Exception {
        long size = write("abcd\nabcd\r\n");

        List<ObjectSlice> shard0 = assign(size, 0, 2);
        List<ObjectSlice> shard1 = assign(size, 1, 2);

        assertThat(shard0, contains(new ObjectSlice(NAME, 0, 11, 11)));
        assertThat(shard1, is(empty()));
        assertThat(read("equal_bytes", shard0), contains("abcd", "abcd"));
        assertThat(readWholeFile(size), contains("abcd", "abcd"));
    }

    public void testLfLineOfExactlyMaxRecordBytesStartingAtTheCutIsOwnedByThePreviousShard() throws Exception {
        long size = write("abcd\nabcd\n");

        List<ObjectSlice> shard0 = assign(size, 0, 2);
        List<ObjectSlice> shard1 = assign(size, 1, 2);

        assertThat(shard0, contains(new ObjectSlice(NAME, 0, 10, 10)));
        assertThat(shard1, is(empty()));
        assertThat(read("equal_bytes", shard0), contains("abcd", "abcd"));
    }

    public void testCrlfLineOfMaxRecordBytesPlusOneAtTheCutIsRejectedByTheFramerLikeWholeFiles() throws Exception {
        long size = write("abcd\r\nabcde\r\n");

        List<ObjectSlice> shard0 = assign(size, 0, 2);
        List<ObjectSlice> shard1 = assign(size, 1, 2);

        assertThat(shard0, contains(new ObjectSlice(NAME, 0, 13, 13)));
        assertThat(shard1, is(empty()));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> read("equal_bytes", shard0));
        assertThat(e.getCause().getMessage(), containsString("record starting at offset 6 exceeds max_record_bytes [4]"));
        UncheckedIOException whole = expectThrows(UncheckedIOException.class, () -> readWholeFile(size));
        assertThat(whole.getCause().getMessage(), containsString("record starting at offset 6 exceeds max_record_bytes [4]"));
    }

    public void testLfLineOfMaxRecordBytesPlusOneAtTheCutIsRejectedByTheFramerLikeWholeFiles() throws Exception {
        long size = write("abcd\r\nabcde\n");

        List<ObjectSlice> shard0 = assign(size, 0, 2);
        List<ObjectSlice> shard1 = assign(size, 1, 2);

        assertThat(shard0, contains(new ObjectSlice(NAME, 0, 12, 12)));
        assertThat(shard1, is(empty()));
        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> read("equal_bytes", shard0));
        assertThat(e.getCause().getMessage(), containsString("record starting at offset 6 exceeds max_record_bytes [4]"));
    }

    public void testLineLongerThanEveryAcceptedLineAtTheCutIsStillNotSplittable() throws Exception {
        long size = write("abcdefg\nabcdefgh\n");

        IOException e = expectThrows(IOException.class, () -> assign(size, 0, 2));
        assertThat(e.getMessage(), containsString("has no newline within [8] of offset [8]"));
    }

    public void testBomFirstLineOfExactlyMaxRecordBytesWithCrlfResolvesACutInsideIt() throws Exception {
        long size = write(BOM + "abcd\r\nab\n");

        assertThat(assign(size, 0, (int) size), contains(new ObjectSlice(NAME, 0, 9, size)));
        assertThat(assign(size, 1, (int) size), is(empty()));
        assertThat(readWholeFile(size), contains("abcd", "ab"));
    }

    public void testSlicesAreDisjointCompleteAndReadLikeWholeFilesForEveryShardCount() throws Exception {
        for (String content : List.of(
            "abcd\nabcd\r\n",
            "abcd\r\nabcd\r\n",
            "abcd\nabcd\n",
            "abcd\nabcd",
            "a\r\nabcd\r\nabcd\n",
            BOM + "abcd\r\nab\n",
            BOM + "abcd\nabcd\r\n\r\nab"
        )) {
            assertDisjointCompleteAndEquivalent(content);
        }
    }
}
