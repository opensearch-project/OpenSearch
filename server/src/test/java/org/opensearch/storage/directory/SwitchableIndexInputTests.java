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
import org.opensearch.common.lucene.store.InputStreamIndexInput;
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
import java.nio.file.Path;
import java.util.ArrayList;
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
        return new MockSwitchableIndexInput(
            "switchable",
            FILE_NAME,
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            false,
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
        List<Consumer<SwitchableIndexInput>> operations = new ArrayList<>();
        operations.add(SwitchableIndexInput::getFilePointer);
        operations.add(SwitchableIndexInput::clone);
        operations.add(SwitchableIndexInput::length);
        operations.add(indexInput -> {
            try {
                indexInput.switchToRemote();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
