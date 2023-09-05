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
import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.io.Streams;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
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
