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
public class BlockUnpinningDirectoryTests extends OpenSearchTestCase {

    private static final String FILE_NAME = "test_file";
    private static final int FILE_SIZE = 1024;

    private TransferManager transferManager;
    private FSDirectory fsDirectory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transferManager = mock(TransferManager.class);
        Path path = createTempDir("BlockUnpinningDirectoryTests");
        fsDirectory = new MMapDirectory(path, SimpleFSLockFactory.INSTANCE);

        doAnswer(invocation -> {
            BlobFetchRequest request = invocation.getArgument(0);
            return request.getDirectory().openInput(request.getFileName(), IOContext.READONCE);
        }).when(transferManager).fetchBlob(any());

        initBlockFile(FILE_SIZE);
    }

    public void testOpenInputTracksBlockIndexInput() throws IOException {
        BlockUnpinningDirectory dir = new BlockUnpinningDirectory(createDirectoryWithFile(FILE_SIZE));

        IndexInput input = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        assertTrue(input instanceof OnDemandBlockSnapshotIndexInput);

        input.seek(50);
        assertEquals(50L, input.getFilePointer());

        dir.unpinAndStopTracking();
        assertEquals(0L, input.getFilePointer());

        input.close();
        dir.close();
    }

    public void testOpenInputPassesThroughNonBlockInput() throws IOException {
        byte[] content = new byte[] { 1, 2, 3 };
        BlockUnpinningDirectory dir = new BlockUnpinningDirectory(createDirectoryWithVirtualFile(content));

        IndexInput input = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        assertTrue(input instanceof ByteArrayIndexInput);

        dir.unpinAndStopTracking();

        input.close();
        dir.close();
    }

    public void testSlicesAreTrackedViaCallback() throws IOException {
        BlockUnpinningDirectory dir = new BlockUnpinningDirectory(createDirectoryWithFile(FILE_SIZE));

        IndexInput master = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        IndexInput slice1 = master.slice("slice1", 0, 100);
        IndexInput slice2 = master.slice("slice2", 100, 100);

        master.seek(50);
        slice1.seek(10);
        slice2.seek(20);
        assertTrue(master.getFilePointer() > 0);
        assertTrue(slice1.getFilePointer() > 0);
        assertTrue(slice2.getFilePointer() > 0);

        dir.unpinAndStopTracking();

        assertEquals(0L, master.getFilePointer());
        assertEquals(0L, slice1.getFilePointer());
        assertEquals(0L, slice2.getFilePointer());

        slice1.close();
        slice2.close();
        master.close();
        dir.close();
    }

    public void testUnpinAndStopTrackingIsIdempotent() throws IOException {
        BlockUnpinningDirectory dir = new BlockUnpinningDirectory(createDirectoryWithFile(FILE_SIZE));

        IndexInput input = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        input.seek(50);

        dir.unpinAndStopTracking();
        dir.unpinAndStopTracking();

        input.close();
        dir.close();
    }

    public void testNoTrackingAfterUnpinAndStopTracking() throws IOException {
        BlockUnpinningDirectory dir = new BlockUnpinningDirectory(createDirectoryWithFile(FILE_SIZE));

        IndexInput input1 = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        input1.seek(0);
        input1.readByte();
        dir.unpinAndStopTracking();

        IndexInput input2 = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        input2.seek(10);
        assertEquals(10L, input2.getFilePointer());

        input1.close();
        input2.close();
        dir.close();
    }

    public void testMultipleInputsAllUnpinned() throws IOException {
        BlockUnpinningDirectory dir = new BlockUnpinningDirectory(createDirectoryWithFile(FILE_SIZE));

        IndexInput input1 = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        IndexInput input2 = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        input1.seek(10);
        input2.seek(20);

        dir.unpinAndStopTracking();

        assertEquals(0L, input1.getFilePointer());
        assertEquals(0L, input2.getFilePointer());

        input1.close();
        input2.close();
        dir.close();
    }

    private void initBlockFile(int size) throws IOException {
        String blockName = FILE_NAME + "_block_0";
        try (var output = fsDirectory.createOutput(blockName, IOContext.DEFAULT)) {
            for (int i = 0; i < size; i++) {
                output.writeByte((byte) 0);
            }
        }
    }

    private RemoteSnapshotDirectory createDirectoryWithFile(int size) {
        FileInfo fileInfo = new FileInfo(FILE_NAME, new StoreFileMetadata(FILE_NAME, size, "", Version.LATEST), null);
        BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("snap", 1, Collections.singletonList(fileInfo), 0, 0, 0, 0);
        return new RemoteSnapshotDirectory(snapshot, fsDirectory, transferManager);
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
}
