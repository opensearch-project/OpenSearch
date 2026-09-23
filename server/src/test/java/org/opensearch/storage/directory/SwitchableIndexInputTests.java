/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.filecache.FullFileCachedIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.indexinput.OnDemandPrefetchBlockSnapshotIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInput;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.opensearch.index.store.remote.utils.FileTypeUtils.BLOCK_FILE_IDENTIFIER;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SwitchableIndexInput}.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class SwitchableIndexInputTests extends TieredStorageBaseTestCase {

    FSDirectory localDirectory;
    FileCache fileCache;
    TransferManager transferManager;
    private static final String FILE_NAME = "_0.si";
    private static final String FILE_NAME_BLOCK = "_0.si_block_0";

    private static final Supplier<TieredStoragePrefetchSettings> MOCK_PREFETCH_SETTINGS_SUPPLIER = () -> {
        TieredStoragePrefetchSettings settings = mock(TieredStoragePrefetchSettings.class);
        when(settings.getReadAheadBlockCount()).thenReturn(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
        when(settings.getReadAheadEnableFileFormats()).thenReturn(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS);
        when(settings.isStoredFieldsPrefetchEnabled()).thenReturn(true);
        return settings;
    };

    private Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return MOCK_PREFETCH_SETTINGS_SUPPLIER;
    }

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        populateData();
        localDirectory = FSDirectory.open(createTempDir());
        syncLocalAndRemoteForFile(localDirectory, FILE_NAME);
        int concurrencyLevel = randomIntBetween(1, 2);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, concurrencyLevel);
        transferManager = new TransferManager(
            (name, position, length) -> new InputStreamIndexInput(
                this.remoteSegmentStoreDirectory.openBlockInput(name, position, length, IOContext.DEFAULT),
                length
            ),
            fileCache,
            threadPool
        );
    }

    public void testSwitchableIndexInputLocal() throws IOException {
        assertNull(getFileCacheEntry(FILE_NAME));

        SwitchableIndexInput switchableIndexInput = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            false,
            threadPool,
            getPrefetchSettingsSupplier()
        );

        CachedIndexInput cachedIndexInput = getFileCacheEntry(FILE_NAME);
        assertNotNull(cachedIndexInput);

        IndexInput indexInput = cachedIndexInput.getIndexInput();
        assertTrue(indexInput instanceof FullFileCachedIndexInput);
        assertFalse(switchableIndexInput.isCachedFromRemote());

        IndexInput localIndexInput = localDirectory.openInput(FILE_NAME, IOContext.DEFAULT);
        assertEquals(switchableIndexInput.length(), localIndexInput.length());
        assertEquals(switchableIndexInput.getFilePointer(), localIndexInput.getFilePointer());
        assertEquals(switchableIndexInput.readByte(), localIndexInput.readByte());

        testCloneSliceRefCounting(switchableIndexInput, FILE_NAME);
    }

    public void testSwitchableIndexInputRemote() throws IOException {
        populateData();

        assertNull(getFileCacheEntry(FILE_NAME_BLOCK));

        SwitchableIndexInput switchableIndexInput = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            true,
            threadPool,
            getPrefetchSettingsSupplier()
        );

        CachedIndexInput cachedIndexInput = getFileCacheEntry(FILE_NAME_BLOCK);
        assertNull(cachedIndexInput);

        SwitchableIndexInput switchableIndexInput1 = switchableIndexInput.clone();
        byte b0 = switchableIndexInput1.readByte();

        cachedIndexInput = getFileCacheEntry(FILE_NAME_BLOCK);
        assertNotNull(cachedIndexInput);

        assertTrue(switchableIndexInput.isCachedFromRemote());

        IndexInput remoteIndexInput = remoteSegmentStoreDirectory.openInput(FILE_NAME, IOContext.DEFAULT);
        byte b1 = remoteIndexInput.readByte();
        assertEquals(switchableIndexInput1.length(), remoteIndexInput.length());
        assertEquals(switchableIndexInput1.getFilePointer(), remoteIndexInput.getFilePointer());
        assertEquals(b0, b1);

        switchableIndexInput1.close();

        testCloneSliceRefCounting(switchableIndexInput, FILE_NAME_BLOCK);
    }

    public void testSwitchToRemote() throws IOException {
        SwitchableIndexInput switchableIndexInput = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            false,
            threadPool,
            getPrefetchSettingsSupplier()
        );

        SwitchableIndexInput clonedIndexInput = switchableIndexInput.clone();
        SwitchableIndexInput slicedIndexInput = switchableIndexInput.slice("slice", 0, switchableIndexInput.length());

        assertFalse(switchableIndexInput.hasSwitchedToRemote());
        assertFalse(clonedIndexInput.hasSwitchedToRemote());
        assertFalse(slicedIndexInput.hasSwitchedToRemote());
        assertNotNull(getFileCacheEntry(FILE_NAME));
        assertNull(getFileCacheEntry(FILE_NAME_BLOCK));

        long filePointerBeforeSwitching = switchableIndexInput.getFilePointer();
        switchableIndexInput.switchToRemote();
        long filePointerAfterSwitching = switchableIndexInput.getFilePointer();

        switchableIndexInput.readByte();
        assertTrue(switchableIndexInput.hasSwitchedToRemote());
        assertTrue(clonedIndexInput.hasSwitchedToRemote());
        assertTrue(slicedIndexInput.hasSwitchedToRemote());
        assertNull(getFileCacheEntry(FILE_NAME));
        assertNotNull(getFileCacheEntry(FILE_NAME_BLOCK));

        assertEquals(filePointerAfterSwitching, filePointerBeforeSwitching);
    }

    public void testPrefetch() throws IOException {
        SwitchableIndexInput switchableIndexInput = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            false,
            threadPool,
            getPrefetchSettingsSupplier()
        );

        switchableIndexInput.prefetch(0, 10);
        IndexInput indexInput = switchableIndexInput.getUnderlyingIndexInput();
        assertTrue(indexInput instanceof FullFileCachedIndexInput);
        switchableIndexInput.switchToRemote();
        assertTrue(switchableIndexInput.hasSwitchedToRemote());
        switchableIndexInput.prefetch(0, 10);
        indexInput = switchableIndexInput.getUnderlyingIndexInput();
        assertTrue(indexInput instanceof OnDemandPrefetchBlockSnapshotIndexInput);
    }

    public void testConcurrencySingleIndexInput() throws IOException, InterruptedException {
        MockSwitchableIndexInput switchableIndexInput = getMockSwitchableIndexInput();
        List<SwitchableIndexInput> indexInputs = new ArrayList<>();
        indexInputs.add(switchableIndexInput);
        final ExecutorService testRunner = Executors.newFixedThreadPool(8);
        try {
            InjectableLock objectLock = switchableIndexInput.getObjectLock();
            List<Consumer<SwitchableIndexInput>> operations = getOperationsToExecute();
            runOperationsConcurrently(testRunner, operations, indexInputs, 10, true);
            objectLock.setDelayEnabled(true);
            runOperationsConcurrently(testRunner, operations, indexInputs, 10, false);
            objectLock.setDelayEnabled(false);
        } finally {
            assertTrue(terminate(testRunner));
        }
    }

    public void testConcurrencyMultipleIndexInput() throws IOException, InterruptedException {
        MockSwitchableIndexInput switchableIndexInput = getMockSwitchableIndexInput();
        SwitchableIndexInput clone1 = switchableIndexInput.clone();
        SwitchableIndexInput clone2 = clone1.clone();
        List<SwitchableIndexInput> indexInputs = new ArrayList<>();
        indexInputs.add(clone1);
        indexInputs.add(clone2);
        final ExecutorService testRunner = Executors.newFixedThreadPool(8);
        try {
            InjectableReadWriteLock sharedLock = switchableIndexInput.getSharedLock();
            List<Consumer<SwitchableIndexInput>> operations = getOperationsToExecute();
            runOperationsConcurrently(testRunner, operations, indexInputs, 10, true);
            sharedLock.setWriteDelayEnabled(true);
            runOperationsConcurrently(testRunner, operations, indexInputs, 10, false);
            sharedLock.setWriteDelayEnabled(false);
        } finally {
            assertTrue(terminate(testRunner));
        }
    }

    private SwitchableIndexInput newHotRemoteIndexInput() throws IOException {
        return new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            true,
            true,
            threadPool,
            getPrefetchSettingsSupplier()
        );
    }

    // replaces the local copy of FILE_NAME with `length` bytes all equal to `value`, so local reads are distinguishable from remote (zeros)
    private void overwriteLocalFile(byte value, long length) throws IOException {
        Files.deleteIfExists(getFilePath(localDirectory, FILE_NAME));
        try (IndexOutput output = localDirectory.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            byte[] buffer = new byte[8192];
            Arrays.fill(buffer, value);
            long pos = 0;
            while (pos < length) {
                int size = (int) Math.min(buffer.length, length - pos);
                output.writeBytes(buffer, 0, size);
                pos += size;
            }
        }
    }

    public void testSwitchToLocal() throws IOException {
        populateData();
        long fileLength = remoteSegmentStoreDirectory.fileLength(FILE_NAME);
        overwriteLocalFile((byte) 42, fileLength);

        SwitchableIndexInput root = newHotRemoteIndexInput();
        assertTrue(root.canSwitchToLocal());
        assertNull(getFileCacheEntry(FILE_NAME_BLOCK));
        // clone()/slice() seek the remote input, which already demands block 0
        SwitchableIndexInput clone = root.clone();
        SwitchableIndexInput slice = root.slice("slice", 1, fileLength - 1);
        for (SwitchableIndexInput input : List.of(root, clone, slice)) {
            assertTrue(input.hasSwitchedToRemote());
            assertFalse(input.isStable());
        }

        // remote bytes are zeros
        assertEquals(0, clone.readByte());
        assertNotNull(getFileCacheEntry(FILE_NAME_BLOCK));
        assertTrue(Files.exists(getFilePath(localDirectory, FILE_NAME_BLOCK)));
        root.seek(3);
        slice.seek(2);

        root.switchToLocal();

        for (SwitchableIndexInput input : List.of(root, clone, slice)) {
            assertFalse(input.hasSwitchedToRemote());
            assertTrue(input.isStable());
            assertFalse(input.getUnderlyingIndexInput() instanceof OnDemandPrefetchBlockSnapshotIndexInput);
        }
        // file pointers survive the switch and reads now come from the local file
        assertEquals(3, root.getFilePointer());
        assertEquals(1, clone.getFilePointer());
        assertEquals(2, slice.getFilePointer());
        assertEquals(42, root.readByte());
        assertEquals(42, clone.readByte());
        assertEquals(42, slice.readByte());
        assertEquals(fileLength, root.length());
        assertEquals(fileLength, clone.length());
        assertEquals(fileLength - 1, slice.length());
        // block entry and block file are gone
        assertNull(getFileCacheEntry(FILE_NAME_BLOCK));
        assertFalse(Files.exists(getFilePath(localDirectory, FILE_NAME_BLOCK)));

        // clones taken after promotion are local and stable from the start
        SwitchableIndexInput lateClone = root.clone();
        assertFalse(lateClone.hasSwitchedToRemote());
        assertTrue(lateClone.isStable());
        assertEquals(root.getFilePointer(), lateClone.getFilePointer());
        assertEquals(42, lateClone.readByte());

        // idempotent
        root.switchToLocal();
        assertEquals(4, root.getFilePointer());
        assertEquals(42, root.readByte());

        lateClone.close();
        slice.close();
        clone.close();
        root.close();
        root.switchToLocal(); // no-op on a closed input
    }

    public void testSwitchDirectionIsFixedAtConstruction() throws IOException {
        populateData();
        // warm-style input: remote is terminal, switchToLocal is refused
        SwitchableIndexInput warm = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            true,
            threadPool,
            getPrefetchSettingsSupplier()
        );
        assertFalse(warm.canSwitchToLocal());
        assertTrue(warm.isStable());
        expectThrows(IllegalStateException.class, warm::switchToLocal);
        assertTrue(warm.hasSwitchedToRemote());
        warm.close();

        // hot-style input: local is terminal, switchToRemote is refused, and switchToLocal is root-only
        SwitchableIndexInput hot = newHotRemoteIndexInput();
        SwitchableIndexInput hotClone = hot.clone();
        expectThrows(IllegalStateException.class, hot::switchToRemote);
        expectThrows(IllegalStateException.class, hotClone::switchToLocal);
        assertTrue(hot.hasSwitchedToRemote());
        assertTrue(hotClone.hasSwitchedToRemote());
        hotClone.close();
        hot.close();
    }

    public void testSwitchToLocalRequiresCompleteLocalFile() throws IOException {
        populateData();
        long fileLength = remoteSegmentStoreDirectory.fileLength(FILE_NAME);
        SwitchableIndexInput root = newHotRemoteIndexInput();
        SwitchableIndexInput clone = root.clone();
        assertEquals(0, clone.readByte());

        localDirectory.deleteFile(FILE_NAME);
        IllegalStateException missing = expectThrows(IllegalStateException.class, root::switchToLocal);
        assertTrue(missing.getMessage(), missing.getMessage().contains("not present locally"));

        overwriteLocalFile((byte) 42, fileLength - 1);
        IllegalStateException truncated = expectThrows(IllegalStateException.class, root::switchToLocal);
        assertTrue(truncated.getMessage(), truncated.getMessage().contains("local length"));

        // still remote, still readable, block entry untouched
        assertTrue(root.hasSwitchedToRemote());
        assertTrue(clone.hasSwitchedToRemote());
        assertEquals(0, clone.readByte());
        assertNotNull(getFileCacheEntry(FILE_NAME_BLOCK));

        overwriteLocalFile((byte) 42, fileLength);
        root.switchToLocal();
        assertFalse(clone.hasSwitchedToRemote());
        assertEquals(42, clone.readByte());
        clone.close();
        root.close();
    }

    public void testStableFollowsTerminalState() throws IOException {
        // hot: a locally written file that can switch to local is already terminal
        SwitchableIndexInput hotLocal = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            false,
            true,
            threadPool,
            getPrefetchSettingsSupplier()
        );
        assertFalse(hotLocal.hasSwitchedToRemote());
        assertTrue(hotLocal.isStable());
        hotLocal.close();
        uploadToRemote(FILE_NAME);

        // warm: local start is not stable, remote (after switchToRemote) is
        SwitchableIndexInput warm = new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            false,
            threadPool,
            getPrefetchSettingsSupplier()
        );
        assertFalse(warm.isStable());
        warm.switchToRemote();
        assertTrue(warm.isStable());
        warm.close();
    }

    public void testBlockCount() {
        long blockSize = 1L << AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE_SHIFT;
        assertEquals(0, SwitchableIndexInput.blockCount(0));
        assertEquals(1, SwitchableIndexInput.blockCount(1));
        assertEquals(1, SwitchableIndexInput.blockCount(blockSize));
        assertEquals(2, SwitchableIndexInput.blockCount(blockSize + 1));
        assertEquals(3, SwitchableIndexInput.blockCount(3 * blockSize));
        assertEquals(4, SwitchableIndexInput.blockCount(3 * blockSize + 1));
    }

    public void testConcurrencySwitchToLocal() throws IOException, InterruptedException {
        populateData();
        MockSwitchableIndexInput root = getMockSwitchableIndexInput(true, true);
        SwitchableIndexInput clone1 = root.clone();
        SwitchableIndexInput clone2 = clone1.clone();
        List<SwitchableIndexInput> indexInputs = List.of(clone1, clone2);
        List<Consumer<SwitchableIndexInput>> operations = getOperationsToExecute(indexInput -> {
            try {
                root.switchToLocal();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        final ExecutorService testRunner = Executors.newFixedThreadPool(8);
        try {
            InjectableLock objectLock = root.getObjectLock();
            runOperationsConcurrently(testRunner, operations, indexInputs, 10, true);
            assertFalse(root.hasSwitchedToRemote());
            assertFalse(clone1.hasSwitchedToRemote());
            assertFalse(clone2.hasSwitchedToRemote());
            assertNull(getFileCacheEntry(FILE_NAME_BLOCK));
            // switchToLocal always takes the root's object lock even once local, so a delayed lock stalls the batch
            objectLock.setDelayEnabled(true);
            runOperationsConcurrently(testRunner, operations, indexInputs, 10, false);
            objectLock.setDelayEnabled(false);
        } finally {
            assertTrue(terminate(testRunner));
        }
    }

    private void testCloneSliceRefCounting(SwitchableIndexInput switchableIndexInput, String fileName) throws IOException {
        IndexInput clonedIndexInput = switchableIndexInput.clone();
        IndexInput slicedIndexInput = switchableIndexInput.slice("slice", 0, switchableIndexInput.length());
        fileCache.prune();
        long parentPointer = switchableIndexInput.getFilePointer();

        assertEquals(switchableIndexInput.getFilePointer(), clonedIndexInput.getFilePointer());
        assertEquals(slicedIndexInput.getFilePointer(), 0);

        clonedIndexInput.seek(clonedIndexInput.getFilePointer() + 1);
        slicedIndexInput.seek(slicedIndexInput.getFilePointer() + 2);

        assertNotEquals(switchableIndexInput.getFilePointer(), clonedIndexInput.getFilePointer());
        assertNotEquals(switchableIndexInput.getFilePointer(), slicedIndexInput.getFilePointer());
        assertEquals(switchableIndexInput.getFilePointer(), parentPointer);

        CachedIndexInput cachedIndexInput = getFileCacheEntry(fileName);
        assertNotNull(cachedIndexInput);

        clonedIndexInput.close();
        slicedIndexInput.close();

        cachedIndexInput = getFileCacheEntry(fileName);
        assertNotNull(cachedIndexInput);

        if (fileName.contains(BLOCK_FILE_IDENTIFIER) == false) {
            uploadToRemote(fileName);
        }

        fileCache.prune();

        cachedIndexInput = getFileCacheEntry(fileName);
        assertNull(cachedIndexInput);
    }

    private CachedIndexInput getFileCacheEntry(String name) {
        Path path = getFilePath(localDirectory, name);
        CachedIndexInput cachedIndexInput = fileCache.get(path);
        fileCache.decRef(path);
        return cachedIndexInput;
    }

    private void uploadToRemote(String file) {
        fileCache.decRef(getFilePath(localDirectory, file));
        fileCache.decRef(getFilePathSwitchable(localDirectory, file));
    }

    private MockSwitchableIndexInput getMockSwitchableIndexInput() throws IOException {
        return getMockSwitchableIndexInput(false, false);
    }

    private MockSwitchableIndexInput getMockSwitchableIndexInput(boolean cacheFromRemote, boolean canSwitchToLocal) throws IOException {
        return new MockSwitchableIndexInput(
            "switchable",
            FILE_NAME,
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            cacheFromRemote,
            canSwitchToLocal,
            threadPool
        );
    }

    private Runnable createOperationRunner(
        CountDownLatch startTogether,
        Consumer<SwitchableIndexInput> operation,
        SwitchableIndexInput switchableIndexInput,
        CountDownLatch latch
    ) {
        return () -> {
            try {
                startTogether.await();
                operation.accept(switchableIndexInput);
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                latch.countDown();
            }
        };
    }

    private List<Consumer<SwitchableIndexInput>> getOperationsToExecute() {
        return getOperationsToExecute(indexInput -> {
            try {
                indexInput.switchToRemote();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Consumer<SwitchableIndexInput>> getOperationsToExecute(Consumer<SwitchableIndexInput> switchOperation) {
        List<Consumer<SwitchableIndexInput>> operations = new ArrayList<>();
        operations.add(SwitchableIndexInput::getFilePointer);
        operations.add(SwitchableIndexInput::clone);
        operations.add(SwitchableIndexInput::length);
        operations.add(switchOperation);
        operations.add(indexInput -> {
            try {
                indexInput.readByte();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return operations;
    }

    private void runOperationsConcurrently(
        ExecutorService testRunner,
        List<Consumer<SwitchableIndexInput>> operations,
        List<SwitchableIndexInput> indexInputs,
        int numTimesToExecute,
        boolean shouldComplete
    ) throws InterruptedException {
        for (int i = 0; i <= numTimesToExecute; i++) {
            CountDownLatch latch = new CountDownLatch(indexInputs.size() * operations.size());
            CountDownLatch startTogether = new CountDownLatch(1);
            for (Consumer<SwitchableIndexInput> operation : operations) {
                for (SwitchableIndexInput indexInput : indexInputs) {
                    testRunner.submit(createOperationRunner(startTogether, operation, indexInput, latch));
                }
            }
            startTogether.countDown();
            assertEquals(shouldComplete, latch.await(5, TimeUnit.SECONDS));
        }
    }

    private static class MockSwitchableIndexInput extends SwitchableIndexInput {

        public MockSwitchableIndexInput(
            String resourceDescription,
            String fileName,
            FileCache fileCache,
            FSDirectory localDirectory,
            org.opensearch.index.store.RemoteSegmentStoreDirectory remoteDirectory,
            TransferManager transferManager,
            boolean cacheFromRemote,
            boolean canSwitchToLocal,
            ThreadPool threadPool
        ) throws IOException {
            super(
                resourceDescription,
                fileName,
                getFilePath(localDirectory, FILE_NAME),
                getFilePathSwitchable(localDirectory, FILE_NAME),
                fileCache,
                localDirectory,
                remoteDirectory,
                transferManager,
                cacheFromRemote,
                canSwitchToLocal,
                threadPool,
                MOCK_PREFETCH_SETTINGS_SUPPLIER
            );
            sharedLock = new InjectableReadWriteLock(sharedLock);
            objectLock = new InjectableLock(objectLock);
        }

        InjectableReadWriteLock getSharedLock() {
            return (InjectableReadWriteLock) sharedLock;
        }

        InjectableLock getObjectLock() {
            return (InjectableLock) objectLock;
        }
    }

    private static class InjectableLock implements Lock {
        private final Lock delegate;
        private final Object delayMonitor = new Object();
        private volatile boolean delayEnabled = false;

        public InjectableLock(Lock delegate) {
            this.delegate = delegate;
        }

        public void setDelayEnabled(boolean enabled) {
            synchronized (delayMonitor) {
                delayEnabled = enabled;
                if (!enabled) {
                    delayMonitor.notifyAll();
                }
            }
        }

        private void maybeDelay() {
            synchronized (delayMonitor) {
                while (delayEnabled) {
                    try {
                        delayMonitor.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during delay", e);
                    }
                }
            }
        }

        @Override
        public void lock() {
            delegate.lock();
            maybeDelay();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            delegate.lockInterruptibly();
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public boolean tryLock(long time, java.util.concurrent.TimeUnit unit) throws InterruptedException {
            return delegate.tryLock(time, unit);
        }

        @Override
        public void unlock() {
            delegate.unlock();
        }

        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }
    }

    private static class InjectableReadWriteLock implements ReadWriteLock {
        private final InjectableLock readLockWrapper;
        private final InjectableLock writeLockWrapper;

        public InjectableReadWriteLock(ReadWriteLock delegate) {
            this.readLockWrapper = new InjectableLock(delegate.readLock());
            this.writeLockWrapper = new InjectableLock(delegate.writeLock());
        }

        public void setWriteDelayEnabled(boolean enabled) {
            writeLockWrapper.setDelayEnabled(enabled);
        }

        @Override
        public Lock readLock() {
            return readLockWrapper;
        }

        @Override
        public Lock writeLock() {
            return writeLockWrapper;
        }
    }
}
