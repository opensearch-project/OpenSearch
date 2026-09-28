/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.storage.indexinput.CachedSwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInputWrapper;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.utils.DirectoryUtils;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.opensearch.index.store.remote.utils.FileTypeUtils.BLOCK_FILE_IDENTIFIER;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;
import static org.apache.lucene.index.IndexFileNames.SEGMENTS;

/**
 * Extension of Composite directory to support writable warm and other related features
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredDirectory extends CompositeDirectory {

    private static final Logger logger = LogManager.getLogger(TieredDirectory.class);

    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    public TieredDirectory(
        Directory localDirectory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(localDirectory, remoteDirectory, fileCache, threadPool);
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        String[] localFiles = localDirectory.listAll();
        String[] remoteFiles;

        // Check if local directory has any segments_n files
        boolean hasLocalSegments = Arrays.stream(localFiles).anyMatch(fileName -> fileName.startsWith(IndexFileNames.SEGMENTS));

        try {
            if (hasLocalSegments) {
                // If local has segments_n, filter out segments_n from remote
                remoteFiles = Arrays.stream(remoteDirectory.listAll())
                    .filter(fileName -> !fileName.startsWith(IndexFileNames.SEGMENTS))
                    .toArray(String[]::new);
            } else {
                // If local doesn't have segments_n, include all remote files
                remoteFiles = remoteDirectory.listAll();
            }
        } catch (NullPointerException e) {
            remoteFiles = new String[] {};
        }

        return Stream.concat(Arrays.stream(localFiles), Arrays.stream(remoteFiles))
            .map(s -> s.contains(BLOCK_FILE_IDENTIFIER) ? s.substring(0, s.indexOf(BLOCK_FILE_IDENTIFIER)) : s)
            .distinct()
            .sorted()
            .toArray(String[]::new);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        // If entry doesn't exist, remove will be a NoOp
        fileCache.remove(getFilePathSwitchable(localDirectory, name));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        super.rename(source, dest);
        // remove switchable entry of source
        fileCache.remove(getFilePathSwitchable(localDirectory, source));
        // remove block entries of source (along with the full file) if any from file cache
        for (String file : listBlockFiles(source)) {
            fileCache.remove(getFilePath(file));
        }
    }

    /**
     * Whenever we write a file to the directory we add a switchable entry in the FileCache (logic in cacheFile method below)
     *
     * But for replicas where we download the file from remote, this entry would be missing hence we first check if file is present in remote or not
     * and then depending upon if it is present, we add the switchable entry to the FileCache.
     *
     * For reading any file (that is not .tmp) we rely on this switchable entry in the FileCache
     *
     * Detailed step wise flow below:
     *
     * 1. Check if file is .tmp file - read it from the local directory
     * 2. Check if the file cache contains switchable entry of the file (we create an entry when writing the file to the directory) - clone the entry and read it
     * 3. If 2 is false, then check if file exists in remote or not - if not throw NoSuchFileException since file doesn't exist in directory
     * 4. If file is present in remote, then add a corresponding switchable entry in the file cache. Now once file cache has the entry we can follow step 2
     *
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        // We aren't tracking temporary files (created via createTempOutput) currently in FileCache as these are created and then deleted
        // within a very short span of time
        // We will be reading them directory from the local directory
        if (FileTypeUtils.isTempFile(name) || FileTypeUtils.isCorruptedFile(name)) {
            return localDirectory.openInput(name, context);
        }
        Path key = getFilePathSwitchable(localDirectory, name);
        CachedIndexInput indexInput = fileCache.get(key);
        if (indexInput == null) {
            RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
                .get(name);
            if (uploadedSegmentMetadata == null) {
                throw new NoSuchFileException("File " + name + " not found in directory");
            }
            // For pre-created files already in remote, switchable entry would not be cached already locally, we need to download from
            // remote and cache it in FileCache
            cacheFile(name, true);
            indexInput = fileCache.get(key);
            // decrementing the ref here since refCount increases in cacheFile method, but since afterSyncToRemote won't be called (where we
            // decRef) for files already in remote
            fileCache.decRef(key);
        }
        try {
            return new SwitchableIndexInputWrapper(
                "SwitchableIndexInput (path=" + getFilePath(name) + ")",
                (SwitchableIndexInput) indexInput.getIndexInput().clone()
            );
        } finally {
            fileCache.decRef(key);
        }
    }

    @Override
    public void afterSyncToRemote(String file) {
        super.afterSyncToRemote(file);

        // once file is uploaded to remote, we can decrement the ref count of the switchable entry in file cache
        fileCache.decRef(getFilePathSwitchable(localDirectory, file));

        // perform the full file to block switch for the switchable index input (currently called here for testing)
        switchFileToRemote(file);
    }

    @Override
    public void sync(Collection<String> names) {
        logger.trace("Tiered Directory[{}]: sync() called {}; Skipping sync.", this::toString, () -> names);
    }

    /**
     * Switch the file from local based full-file to remote based blocked-file
     * @param file
     */
    public void switchFileToRemote(String file) {
        Path filePath = getFilePathSwitchable(localDirectory, file);
        try {
            SwitchableIndexInput switchableIndexInput = ((SwitchableIndexInput) fileCache.get(filePath).getIndexInput());
            switchableIndexInput.switchToRemote();
        } catch (IOException | IllegalStateException e) {
            logger.error("Failed to switch full file to blocked - " + file, e);
        } finally {
            fileCache.decRef(filePath);
        }
    }

    @Override
    public void close() throws IOException {
        ensureOpen();
        logger.trace("Stormborn Directory[{}]: close() called", this::toString);
        String[] localFiles = listLocalFiles();
        for (String localFile : localFiles) {
            // Delete segments_N file with ref count 1 created during index creation on replica shards
            // TODO: https://github.com/opensearch-project/OpenSearch/issues/17534
            if (localFile.startsWith(SEGMENTS)) {
                fileCache.remove(getFilePath(localFile));
            }
            String finalFileName = localFile;
            if (localFile.contains(BLOCK_FILE_IDENTIFIER)) {
                String[] segments = localFile.split(BLOCK_FILE_IDENTIFIER);
                finalFileName = segments[0].strip();
            }
            fileCache.remove(getFilePathSwitchable(localDirectory, finalFileName));
        }
        localDirectory.close();
    }

    public String[] listLocalFiles() throws IOException {
        ensureOpen();
        logger.trace("Stormborn Directory[{}]: listLocalOnly() called", this::toString);
        return localDirectory.listAll();
    }

    @Override
    protected void cacheFile(String fileName) throws IOException {
        cacheFile(fileName, false);
    }

    // protected for testing
    protected void cacheFile(String fileName, boolean cacheFromRemote) throws IOException {
        Path switchableFilePath = getFilePathSwitchable(localDirectory, fileName);
        fileCache.put(
            switchableFilePath,
            new CachedSwitchableIndexInput(
                fileCache,
                fileName,
                DirectoryUtils.unwrapFSDirectory(localDirectory),
                remoteDirectory,
                transferManager,
                cacheFromRemote,
                threadPool,
                tieredStoragePrefetchSettingsSupplier
            )
        );
    }
}
