/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.AlreadyClosedException;
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
import java.io.InterruptedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    public void testListAllWaitsForSnapshotLoad() throws Exception {
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> remoteState = new CompletableFuture<>();
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(remoteState, fsDirectory);

        assertWaitsUntilCompleted(remoteState, remoteState(1024), () -> assertArrayEquals(new String[] { FILE_NAME }, dir.listAll()));
        dir.close();
    }

    public void testFileLengthWaitsForSnapshotLoad() throws Exception {
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> remoteState = new CompletableFuture<>();
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(remoteState, fsDirectory);

        assertWaitsUntilCompleted(remoteState, remoteState(1024), () -> assertEquals(1024, dir.fileLength(FILE_NAME)));
        dir.close();
    }

    public void testOpenInputWaitsForSnapshotLoad() throws Exception {
        initBlockFile(1024);
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> remoteState = new CompletableFuture<>();
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(remoteState, fsDirectory);

        assertWaitsUntilCompleted(remoteState, remoteState(1024), () -> {
            try (IndexInput input = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertTrue(input instanceof OnDemandBlockSnapshotIndexInput);
                assertEquals(1024, input.length());
            }
        });
        dir.close();
    }

    public void testFailedSnapshotLoadSurfacesOnFirstUse() throws IOException {
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> remoteState = new CompletableFuture<>();
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(remoteState, fsDirectory);
        IOException cause = new IOException("snap-uuid.dat not found");
        remoteState.completeExceptionally(cause);

        IOException e = expectThrows(IOException.class, dir::listAll);
        assertSame(cause, e.getCause());
        dir.close();
    }

    public void testInterruptWhileWaitingKeepsInterruptFlag() throws Exception {
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(new CompletableFuture<>(), fsDirectory);
        AtomicReference<Exception> thrown = new AtomicReference<>();
        AtomicReference<Boolean> flagAfter = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                dir.listAll();
            } catch (Exception e) {
                thrown.set(e);
                flagAfter.set(Thread.currentThread().isInterrupted());
            }
        });
        reader.start();
        assertBusy(() -> assertEquals(Thread.State.WAITING, reader.getState()));
        reader.interrupt();
        reader.join(10_000);

        assertFalse(reader.isAlive());
        assertTrue(thrown.get() instanceof InterruptedIOException);
        assertEquals(Boolean.TRUE, flagAfter.get());
        dir.close();
    }

    public void testSnapshotStateIsLoadedOnce() throws Exception {
        AtomicInteger gets = new AtomicInteger();
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> remoteState = new CompletableFuture<>() {
            @Override
            public RemoteSnapshotDirectory.RemoteState get() throws InterruptedException, ExecutionException {
                gets.incrementAndGet();
                return super.get();
            }
        };
        remoteState.complete(remoteState(1024));
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(remoteState, fsDirectory);

        dir.listAll();
        dir.listAll();
        dir.fileLength(FILE_NAME);
        assertEquals(1, gets.get());
        dir.close();
    }

    public void testCloseCancelsPendingSnapshotLoad() throws IOException {
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> remoteState = new CompletableFuture<>();
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(remoteState, fsDirectory);
        dir.close();
        assertTrue(remoteState.isCancelled());
    }

    public void testUseAfterCloseOfPendingSnapshotLoadThrowsAlreadyClosed() throws IOException {
        // a FutureTask that did not run, as the snapshot pool returns it
        FutureTask<RemoteSnapshotDirectory.RemoteState> task = new FutureTask<>(() -> remoteState(1024));
        RemoteSnapshotDirectory dir = new RemoteSnapshotDirectory(task, fsDirectory);
        dir.close();

        AlreadyClosedException e = expectThrows(AlreadyClosedException.class, dir::listAll);
        assertTrue(e.getCause() instanceof CancellationException);
    }

    /** Runs the check on another thread, shows that it waits while the future is open, then completes the future. */
    private void assertWaitsUntilCompleted(
        CompletableFuture<RemoteSnapshotDirectory.RemoteState> future,
        RemoteSnapshotDirectory.RemoteState state,
        Check check
    ) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            try {
                check.run();
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                done.countDown();
            }
        });
        reader.start();
        assertFalse("the call must wait for the shard snapshot", done.await(100, TimeUnit.MILLISECONDS));

        future.complete(state);
        assertTrue(done.await(10, TimeUnit.SECONDS));
        reader.join();
        assertNull(failure.get());
    }

    @FunctionalInterface
    private interface Check {
        void run() throws Exception;
    }

    private RemoteSnapshotDirectory.RemoteState remoteState(int size) {
        FileInfo fileInfo = new FileInfo(FILE_NAME, new StoreFileMetadata(FILE_NAME, size, "", Version.LATEST), null);
        BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("snap", 1, Collections.singletonList(fileInfo), 0, 0, 0, 0);
        return new RemoteSnapshotDirectory.RemoteState(snapshot, transferManager);
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
