/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class RemoteSnapshotDirectoryTests extends OpenSearchTestCase {

    private static final String FILE_NAME = "test_file";

    private TransferManager transferManager;
    private FSDirectory fsDirectory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transferManager = mock(TransferManager.class);
        Path path = createTempDir("RemoteSnapshotDirectoryTests");
        fsDirectory = new MMapDirectory(path, SimpleFSLockFactory.INSTANCE);

        doAnswer(invocation -> {
            BlobFetchRequest request = invocation.getArgument(0);
            return request.getDirectory().openInput(request.getFileName(), IOContext.READONCE);
        }).when(transferManager).fetchBlob(any());
    }

    public void testOpenInputReturnsOnDemandBlockSnapshotIndexInput() throws IOException {
        initBlockFile(1024);
        RemoteSnapshotDirectory dir = createDirectoryWithFile(1024);
        IndexInput input = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        assertTrue(input instanceof OnDemandBlockSnapshotIndexInput);
        assertEquals(1024, input.length());
        byte[] readBack = new byte[1024];
        input.readBytes(readBack, 0, 1024);
        assertArrayEquals(new byte[1024], readBack);
        input.close();
        dir.close();
    }

    public void testOpenInputVirtualFileReturnsByteArrayIndexInput() throws IOException {
        byte[] content = new byte[] { 1, 2, 3, 4, 5 };
        RemoteSnapshotDirectory dir = createDirectoryWithVirtualFile(content);

        IndexInput input = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        assertTrue(input instanceof ByteArrayIndexInput);
        assertEquals(content.length, input.length());
        byte[] readBack = new byte[content.length];
        input.readBytes(readBack, 0, content.length);
        assertArrayEquals(content, readBack);
        input.close();
        dir.close();
    }

    private RemoteSnapshotDirectory createDirectoryWithVirtualFile(byte[] content) {
        FileInfo virtualFile = new FileInfo(
            "v__virtual",
            new StoreFileMetadata(FILE_NAME, content.length, "", Version.LATEST, new BytesRef(content)),
            null
        );
        BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(
            "snap",
            1,
            Collections.singletonList(virtualFile),
            0,
            0,
            0,
            0
        );
        return new RemoteSnapshotDirectory(snapshot, fsDirectory, transferManager);
    }

    private RemoteSnapshotDirectory createDirectoryWithFile(int size) {
        FileInfo fileInfo = new FileInfo(FILE_NAME, new StoreFileMetadata(FILE_NAME, size, "", Version.LATEST), null);
        BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("snap", 1, Collections.singletonList(fileInfo), 0, 0, 0, 0);
        return new RemoteSnapshotDirectory(snapshot, fsDirectory, transferManager);
    }

    private void initBlockFile(int size) throws IOException {
        String blockName = FILE_NAME + "_block_0";
        try (var output = fsDirectory.createOutput(blockName, IOContext.DEFAULT)) {
            for (int i = 0; i < size; i++) {
                output.writeByte((byte) 0);
            }
        }
    }
}
