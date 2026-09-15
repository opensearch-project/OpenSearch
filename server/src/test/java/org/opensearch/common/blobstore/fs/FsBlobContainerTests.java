/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.blobstore.fs;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterSeekableByteChannel;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.io.Streams;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("*") // we do our own mocking
public class FsBlobContainerTests extends OpenSearchTestCase {

    final AtomicLong totalBytesRead = new AtomicLong(0);
    FileSystem fileSystem = null;

    @Before
    public void setupMockFileSystems() {
        FileSystemProvider fileSystemProvider = new MockFileSystemProvider(PathUtils.getDefaultFileSystem(), totalBytesRead::addAndGet);
        fileSystem = fileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem); // restored by restoreFileSystem in OpenSearchTestCase
    }

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystem);
    }

    public void testReadBlobRangeCorrectlySkipBytes() throws IOException {
        final String blobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final byte[] blobData = randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb

        final Path path = PathUtils.get(createTempDir().toString());
        Files.write(path.resolve(blobName), blobData);

        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.cleanPath(),
            path
        );
        assertThat(totalBytesRead.get(), equalTo(0L));

        final long start = randomLongBetween(0L, Math.max(0L, blobData.length - 1));
        final long length = randomLongBetween(1L, blobData.length - start);

        try (InputStream stream = container.readBlob(blobName, start, length)) {
            assertThat(totalBytesRead.get(), equalTo(0L));
            assertThat(Streams.consumeFully(stream), equalTo(length));
            assertThat(totalBytesRead.get(), equalTo(length));
        }
    }

    public void testTempBlobName() {
        final String blobName = randomAlphaOfLengthBetween(1, 20);
        final String tempBlobName = FsBlobContainer.tempBlobName(blobName);
        assertThat(tempBlobName, startsWith("pending-"));
        assertThat(tempBlobName, containsString(blobName));
    }

    public void testIsTempBlobName() {
        final String tempBlobName = FsBlobContainer.tempBlobName(randomAlphaOfLengthBetween(1, 20));
        assertThat(FsBlobContainer.isTempBlobName(tempBlobName), is(true));
    }

    /**
     * The conditional-write methods below work, but only within one JVM: a local filesystem has no compare-and-swap, so
     * the precondition is emulated with in-process locks. {@code isConditionalWriteSupported} reports whether the STORE
     * enforces it, which is what remote store fencing needs to exclude a writer on another node, so this container must
     * answer {@code false} however well its emulation behaves here. Two nodes sharing a filesystem would otherwise both
     * believe they held the fence.
     */
    public void testConditionalWriteIsNotReportedAsSupportedBecauseItIsOnlyEmulated() throws IOException {
        assertThat(newContainer().isConditionalWriteSupported(), is(false));
    }

    public void testReadBlobWithVersionOnMissingBlobThrows() throws IOException {
        final FsBlobContainer container = newContainer();
        expectThrows(NoSuchFileException.class, () -> container.readBlobWithVersion("missing"));
    }

    public void testWriteBlobConditionallyCreatesIfAbsent() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));

        final String token = container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);
        assertNotNull(token);

        final VersionedBlob blob = container.readBlobWithVersion("fence");
        assertArrayEquals(content, blob.content());
        assertThat(blob.versionToken(), equalTo(token));
    }

    public void testWriteBlobConditionallyRejectsCreateWhenBlobExists() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);

        // create-if-absent (null expected token) must now fail since the blob is present
        expectThrows(
            BlobVersionConflictException.class,
            () -> container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null)
        );
    }

    public void testWriteBlobConditionallyChainsOnMatchingToken() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] first = randomByteArrayOfLength(randomIntBetween(1, 512));
        String token = container.writeBlobConditionally("fence", new ByteArrayInputStream(first), first.length, null);

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final byte[] next = randomByteArrayOfLength(randomIntBetween(1, 512));
            final String nextToken = container.writeBlobConditionally("fence", new ByteArrayInputStream(next), next.length, token);
            assertNotNull(nextToken);
            final VersionedBlob blob = container.readBlobWithVersion("fence");
            assertArrayEquals(next, blob.content());
            assertThat(blob.versionToken(), equalTo(nextToken));
            token = nextToken;
        }
    }

    public void testWriteBlobConditionallyRejectsStaleToken() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] first = "one".getBytes(StandardCharsets.UTF_8);
        final String staleToken = container.writeBlobConditionally("fence", new ByteArrayInputStream(first), first.length, null);

        final byte[] second = "two".getBytes(StandardCharsets.UTF_8);
        container.writeBlobConditionally("fence", new ByteArrayInputStream(second), second.length, staleToken);

        // The first writer's token is no longer current: its next CAS must be rejected and the blob left untouched
        final byte[] third = "three".getBytes(StandardCharsets.UTF_8);
        expectThrows(
            BlobVersionConflictException.class,
            () -> container.writeBlobConditionally("fence", new ByteArrayInputStream(third), third.length, staleToken)
        );
        assertArrayEquals(second, container.readBlobWithVersion("fence").content());
    }

    /**
     * Version tokens must be content-independent: were they derived from the bytes (e.g. a content hash), two writes
     * of identical content would be indistinguishable, and a stale writer that happens to write the current content
     * would pass a CAS it should lose (ABA).
     */
    public void testIdenticalContentYieldsDistinctVersionTokens() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] content = "same-bytes".getBytes(StandardCharsets.UTF_8);
        final String first = container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);
        final String second = container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, first);
        assertNotEquals("identical content must still produce a new version", first, second);

        // A stale writer whose expected token matches the *content* of the current blob must still lose the CAS
        expectThrows(
            BlobVersionConflictException.class,
            () -> container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, first)
        );
        assertThat(container.readBlobWithVersion("fence").versionToken(), equalTo(second));
    }

    /**
     * Deleting a blob invalidates any previously issued version token, so a blob recreated at the same path is a new
     * version - as it would be under a real object store's ETag semantics. A holder of a pre-delete token must lose
     * its CAS; a fresh read of the recreated blob starts a new chain.
     */
    public void testDeleteInvalidatesVersionTokens() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] content = "one".getBytes(StandardCharsets.UTF_8);
        final String preDeleteToken = container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);

        container.deleteBlobsIgnoringIfNotExists(Collections.singletonList("fence"));
        // recreate at the same path through a non-conditional API
        container.writeBlob("fence", new ByteArrayInputStream(content), content.length, false);

        expectThrows(
            BlobVersionConflictException.class,
            () -> container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, preDeleteToken)
        );
        // a fresh read starts a new chain over the recreated blob
        final String recreatedToken = container.readBlobWithVersion("fence").versionToken();
        assertNotEquals(preDeleteToken, recreatedToken);
        container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, recreatedToken);
    }

    /** Versioned reads are for small control blobs; an oversized blob at the key is refused, not buffered whole. */
    public void testReadBlobWithVersionRejectsOversizedBlob() throws IOException {
        final Path path = PathUtils.get(createTempDir().toString());
        final FsBlobContainer container = new FsBlobContainer(new FsBlobStore(128, path, false), BlobPath.cleanPath(), path);
        final byte[] oversized = randomByteArrayOfLength(129);
        container.writeBlob("fence", new ByteArrayInputStream(oversized), oversized.length, false);
        final IOException e = expectThrows(IOException.class, () -> container.readBlobWithVersion("fence"));
        assertTrue(e.getMessage(), e.getMessage().contains("too large"));
    }

    public void testWriteBlobConditionallyLeavesNoTempBlobs() throws IOException {
        final FsBlobContainer container = newContainer();
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);
        assertThat(container.listBlobsByPrefix("pending-").keySet(), equalTo(Collections.emptySet()));
    }

    public void testConcurrentConditionalWritesAdmitExactlyOneWinner() throws Exception {
        final FsBlobContainer container = newContainer();
        final byte[] initial = "init".getBytes(StandardCharsets.UTF_8);
        final String token = container.writeBlobConditionally("fence", new ByteArrayInputStream(initial), initial.length, null);

        final int writers = randomIntBetween(2, 8);
        final CyclicBarrier barrier = new CyclicBarrier(writers);
        final AtomicInteger succeeded = new AtomicInteger();
        final AtomicInteger conflicted = new AtomicInteger();
        final List<Thread> threads = new ArrayList<>(writers);
        for (int i = 0; i < writers; i++) {
            final byte[] content = ("writer-" + i).getBytes(StandardCharsets.UTF_8);
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    container.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, token);
                    succeeded.incrementAndGet();
                } catch (BlobVersionConflictException e) {
                    conflicted.incrementAndGet();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(succeeded.get(), equalTo(1));
        assertThat(conflicted.get(), equalTo(writers - 1));
    }

    private FsBlobContainer newContainer() throws IOException {
        final Path path = PathUtils.get(createTempDir().toString());
        return new FsBlobContainer(new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false), BlobPath.cleanPath(), path);
    }

    /**
     * Deleting a container must invalidate only ITS OWN blobs' version tokens, never a sibling's whose directory name
     * merely shares the prefix (idx-1 vs idx-10). An over-matched invalidation makes the sibling's live writer lose a
     * CAS it should win - which, for the remote store fence, spuriously fences an unrelated shard.
     */
    public void testDeleteDoesNotInvalidateVersionTokensOfPrefixSiblingContainers() throws IOException {
        final Path root = PathUtils.get(createTempDir().toString());
        final FsBlobStore store = new FsBlobStore(randomIntBetween(1, 8) * 1024, root, false);
        final Path victimPath = root.resolve("idx-1");
        final Path siblingPath = root.resolve("idx-10");
        Files.createDirectories(victimPath);
        Files.createDirectories(siblingPath);
        final FsBlobContainer victim = new FsBlobContainer(store, BlobPath.cleanPath().add("idx-1"), victimPath);
        final FsBlobContainer sibling = new FsBlobContainer(store, BlobPath.cleanPath().add("idx-10"), siblingPath);

        final byte[] content = "fence".getBytes(StandardCharsets.UTF_8);
        victim.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);
        final String siblingToken = sibling.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, null);

        victim.delete();

        // The sibling's chain is untouched: its token still wins the CAS.
        sibling.writeBlobConditionally("fence", new ByteArrayInputStream(content), content.length, siblingToken);
    }

    private void testListBlobsByPrefixInSortedOrder(int limit, BlobContainer.BlobNameSortOrder blobNameSortOrder) throws IOException {

        final Path path = PathUtils.get(createTempDir().toString());

        List<String> blobsInFileSystem = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String blobName = randomAlphaOfLengthBetween(10, 20).toLowerCase(Locale.ROOT);
            final byte[] blobData = randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
            Files.write(path.resolve(blobName), blobData);
            blobsInFileSystem.add(blobName);
        }

        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.cleanPath(),
            path
        );

        if (limit >= 0) {
            container.listBlobsByPrefixInSortedOrder(null, limit, blobNameSortOrder, new ActionListener<>() {
                @Override
                public void onResponse(List<BlobMetadata> blobMetadata) {
                    int actualLimit = Math.min(limit, 10);
                    assertEquals(actualLimit, blobMetadata.size());

                    if (blobNameSortOrder == BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC) {
                        blobsInFileSystem.sort(String::compareTo);
                    } else {
                        blobsInFileSystem.sort(Collections.reverseOrder(String::compareTo));
                    }
                    List<String> keys = blobsInFileSystem.subList(0, actualLimit);
                    assertEquals(keys, blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList()));
                }

                @Override
                public void onFailure(Exception e) {
                    fail("blobContainer.listBlobsByPrefixInLexicographicOrder failed with exception: " + e.getMessage());
                }
            });
        } else {
            assertThrows(
                IllegalArgumentException.class,
                () -> container.listBlobsByPrefixInSortedOrder(null, limit, blobNameSortOrder, new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {}

                    @Override
                    public void onFailure(Exception e) {}
                })
            );
        }
    }

    public void testListBlobsByPrefixInLexicographicOrderWithNegativeLimit() throws IOException {
        testListBlobsByPrefixInSortedOrder(-5, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithZeroLimit() throws IOException {
        testListBlobsByPrefixInSortedOrder(0, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithLimitLessThanNumberOfRecords() throws IOException {
        testListBlobsByPrefixInSortedOrder(8, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithLimitNumberOfRecords() throws IOException {
        testListBlobsByPrefixInSortedOrder(10, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithLimitGreaterThanNumberOfRecords() throws IOException {
        testListBlobsByPrefixInSortedOrder(12, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    static class MockFileSystemProvider extends FilterFileSystemProvider {

        final Consumer<Long> onRead;

        MockFileSystemProvider(FileSystem inner, Consumer<Long> onRead) {
            super("mockfs://", inner);
            this.onRead = onRead;
        }

        private int onRead(int read) {
            if (read != -1) {
                onRead.accept((long) read);
            }
            return read;
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> opts, FileAttribute<?>... attrs) throws IOException {
            return new FilterSeekableByteChannel(super.newByteChannel(path, opts, attrs)) {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return onRead(super.read(dst));
                }
            };
        }

        @Override
        public InputStream newInputStream(Path path, OpenOption... opts) throws IOException {
            // no super.newInputStream(path, opts) as it will use the delegating FileSystem to open a SeekableByteChannel
            // and instead we want the mocked newByteChannel() method to be used
            return new FilterInputStream(delegate.newInputStream(path, opts)) {
                @Override
                public int read() throws IOException {
                    return onRead(super.read());
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return onRead(super.read(b, off, len));
                }
            };
        }
    }
}
