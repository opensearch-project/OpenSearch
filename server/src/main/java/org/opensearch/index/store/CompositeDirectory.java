/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCache.RestoredCachedIndexInput;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.store.remote.utils.FileTypeUtils.BLOCK_FILE_IDENTIFIER;
import static org.apache.lucene.index.IndexFileNames.SEGMENTS;

/**
 * Composite Directory will contain both local and remote directory
 * Consumers of Composite directory need not worry whether file is in local or remote
 * All such abstractions will be handled by the Composite directory itself
 * Implements all required methods by Directory abstraction
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDirectory extends FilterDirectory {
    private static final Logger logger = LogManager.getLogger(CompositeDirectory.class);
    protected final Directory localDirectory;
    protected final RemoteSegmentStoreDirectory remoteDirectory;
    protected final FileCache fileCache;
    protected final TransferManager transferManager;
    protected final ThreadPool threadPool;

    /**
     * Constructor to initialise the composite directory
     * @param localDirectory corresponding to the local FSDirectory
     * @param remoteDirectory corresponding to the remote directory
     * @param fileCache used to cache the remote files locally
     */
    public CompositeDirectory(Directory localDirectory, Directory remoteDirectory, FileCache fileCache, ThreadPool threadPool) {
        super(localDirectory);
        validate(localDirectory, remoteDirectory, fileCache);
        this.localDirectory = localDirectory;
        this.remoteDirectory = (RemoteSegmentStoreDirectory) remoteDirectory;
        this.fileCache = fileCache;
        this.threadPool = threadPool;
        transferManager = new TransferManager(
            (name, position, length) -> new InputStreamIndexInput(
                CompositeDirectory.this.remoteDirectory.openBlockInput(name, position, length, IOContext.DEFAULT),
                length
            ),
            fileCache,
            threadPool
        );
    }

    /**
     * Returns names of all files stored in local directory
     * @throws IOException in case of I/O error
     */
    private String[] listLocalFiles() throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: listLocalOnly() called", this::toString);
        return localDirectory.listAll();
    }

    /**
     * Returns a list of names of all block files stored in the local directory for a given file,
     * including the original file itself if present.
     *
     * @param fileName The name of the file to search for, along with its associated block files.
     * @return A list of file names, including the original file (if present) and all its block files.
     * @throws IOException in case of I/O error while listing files.
     */
    protected List<String> listBlockFiles(String fileName) throws IOException {
        return Stream.of(listLocalFiles())
            .filter(file -> file.equals(fileName) || file.startsWith(fileName + FileTypeUtils.BLOCK_FILE_IDENTIFIER))
            .collect(Collectors.toList());
    }

    /**
     * Returns names of all files stored in this directory in sorted order
     * Does not include locally stored block files (having _block_ in their names) and files pending deletion
     *
     * @throws IOException in case of I/O error
     */
    // TODO: https://github.com/opensearch-project/OpenSearch/issues/17527
    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: listAll() called", this::toString);
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

        logger.trace("Composite Directory[{}]: Local Directory files - {}", this::toString, () -> Arrays.toString(localFiles));
        String[] finalRemoteFiles = remoteFiles;
        logger.trace("Composite Directory[{}]: Remote Directory files - {}", this::toString, () -> Arrays.toString(finalRemoteFiles));
        Set<String> allFiles = Stream.concat(Arrays.stream(localFiles), Arrays.stream(remoteFiles))
            .map(s -> s.contains(BLOCK_FILE_IDENTIFIER) ? s.substring(0, s.indexOf(BLOCK_FILE_IDENTIFIER)) : s)
            .collect(Collectors.toSet());
        Set<String> nonBlockLuceneFiles = allFiles.stream()
            .filter(file -> !FileTypeUtils.isBlockFile(file))
            .collect(Collectors.toUnmodifiableSet());
        String[] files = new String[nonBlockLuceneFiles.size()];
        nonBlockLuceneFiles.toArray(files);
        Arrays.sort(files);
        logger.trace("Composite Directory[{}]: listAll() returns : {}", this::toString, () -> Arrays.toString(files));
        return files;
    }

    /**
     * Removes an existing file in the directory.
     * Currently deleting only from local directory as files from remote should not be deleted as that is taken care by garbage collection logic of remote directory
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException when file does not exist in the directory
     */
    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: deleteFile() called {}", this::toString, () -> name);
        if (FileTypeUtils.isTempFile(name)) {
            localDirectory.deleteFile(name);
        } else if (Arrays.asList(listAll()).contains(name) == false) {
            /*
             We can run into scenarios where stale files are evicted locally (zero refCount in FileCache) and
             not present in remote as well (due to metadata refresh). In such scenarios listAll() of composite directory
             will not show the file. Hence, we need to silently fail deleteFile operation for such files.
             */
            return;
        } else {
            List<String> blockFiles = listBlockFiles(name);
            if (blockFiles.isEmpty()) {
                // Remove this condition when this issue is addressed.
                // TODO: https://github.com/opensearch-project/OpenSearch/issues/17526
                logger.debug("The file [{}] or its block files do not exist in local directory", name);
            } else {
                for (String blockFile : blockFiles) {
                    if (fileCache.get(getFilePath(blockFile)) == null) {
                        logger.debug("The file [{}] exists in local but not part of FileCache, deleting it from local", blockFile);
                        localDirectory.deleteFile(blockFile);
                    } else {
                        fileCache.remove(getFilePath(blockFile));
                    }
                }
            }
        }
    }

    /**
     * Returns the byte length of a file in the directory.
     * Throws {@link NoSuchFileException} or {@link FileNotFoundException} in case file is not present locally and in remote as well
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: fileLength() called {}", this::toString, () -> name);
        long fileLength;
        Path key = getFilePath(name);
        if (FileTypeUtils.isTempFile(name) || fileCache.get(key) != null) {
            try {
                fileLength = localDirectory.fileLength(name);
                logger.trace(
                    "Composite Directory[{}]: fileLength of {} fetched from Local - {}",
                    this::toString,
                    () -> name,
                    () -> fileLength
                );
            } finally {
                fileCache.decRef(key);
            }
        } else {
            fileLength = remoteDirectory.fileLength(name);
            logger.trace(
                "Composite Directory[{}]: fileLength of {} fetched from Remote - {}",
                this::toString,
                () -> name,
                () -> fileLength
            );
        }
        return fileLength;
    }

    /**
     * Creates a new, empty file in the directory and returns an {@link IndexOutput} instance for
     * appending data to this file.
     * @param name the name of the file to create.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: createOutput() called {}", this::toString, () -> name);
        // The CloseableFilterIndexOutput will ensure that the file is added to FileCache once write is completed on this file
        return new CloseableFilterIndexOutput(localDirectory.createOutput(name, context), name, this::cacheFile);
    }

    /**
     * Ensures that any writes to these files are moved to stable storage (made durable).
     * @throws IOException in case of I/O error
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: sync() called {}", this::toString, () -> names);
        Set<String> remoteFiles = Set.of(getRemoteFiles());
        Set<String> localFilesHavingBlocks = Arrays.stream(listLocalFiles())
            .filter(FileTypeUtils::isBlockFile)
            .map(file -> file.substring(0, file.indexOf(BLOCK_FILE_IDENTIFIER)))
            .collect(Collectors.toSet());
        Collection<String> fullFilesToSync = names.stream()
            .filter(name -> (remoteFiles.contains(name) == false) && (localFilesHavingBlocks.contains(name) == false))
            .collect(Collectors.toList());
        logger.trace("Composite Directory[{}]: Synced files : {}", this::toString, () -> fullFilesToSync);
        localDirectory.sync(fullFilesToSync);
    }

    /**
     * Renames {@code source} file to {@code dest} file where {@code dest} must not already exist in
     * the directory.
     * @throws IOException in case of I/O error
     */
    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: rename() called : source-{}, dest-{}", this::toString, () -> source, () -> dest);
        localDirectory.rename(source, dest);
        fileCache.remove(getFilePath(source));
        cacheFile(dest);
    }

    /**
     * Opens a stream for reading an existing file.
     * Check whether the file is present locally or in remote and return the IndexInput accordingly
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: openInput() called {}", this::toString, () -> name);
        // We aren't tracking temporary files (created via createTempOutput) currently in FileCache as these are created and then deleted
        // within a very short span of time
        // We will be reading them directory from the local directory
        if (FileTypeUtils.isTempFile(name)) {
            return localDirectory.openInput(name, context);
        }
        // Return directly from the FileCache (via TransferManager) if complete file is present
        Path key = getFilePath(name);

        CachedIndexInput indexInput = fileCache.compute(key, (path, cachedIndexInput) -> {
            // If entry exists and is not closed, use it
            if (cachedIndexInput != null && cachedIndexInput.isClosed() == false) {
                return cachedIndexInput;
            }

            // If entry is closed but file exists locally, create new IndexInput from local
            if (cachedIndexInput != null && cachedIndexInput.isClosed() && Files.exists(key)) {
                try {
                    assert cachedIndexInput instanceof RestoredCachedIndexInput;
                    return new CachedFullFileIndexInput(fileCache, key, localDirectory.openInput(name, IOContext.DEFAULT));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // Return null to fall back to remote store block download/existing block reuse.
            return null;
        });

        if (indexInput != null) {
            logger.trace("Composite Directory[{}]: Complete file {} found in FileCache", this::toString, () -> name);
            try {
                return indexInput.getIndexInput().clone();
            } finally {
                fileCache.decRef(key);
            }
        }
        // If file has been uploaded to the Remote Store, fetch it from the Remote Store in blocks via OnDemandCompositeBlockIndexInput
        else {
            logger.trace(
                "Composite Directory[{}]: Complete file {} not in FileCache, to be fetched in Blocks from Remote",
                this::toString,
                () -> name
            );
            RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
                .get(name);
            if (uploadedSegmentMetadata == null) {
                throw new NoSuchFileException("File " + name + " not found in directory");
            }
            // TODO : Refactor FileInfo and OnDemandBlockSnapshotIndexInput to more generic names as they are not Remote Snapshot
            // specific
            BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
                name,
                new StoreFileMetadata(name, uploadedSegmentMetadata.getLength(), uploadedSegmentMetadata.getChecksum(), Version.LATEST),
                null
            );
            return new OnDemandBlockSnapshotIndexInput(fileInfo, getLocalFSDirectory(), transferManager);
        }
    }

    /**
     * Closing the local directory here
     * @throws IOException in case of I/O error
     */
    @Override
    public void close() throws IOException {
        ensureOpen();
        logger.trace("Composite Directory[{}]: close() called", this::toString);
        String[] localFiles = listLocalFiles();
        for (String localFile : localFiles) {
            // Delete segments_N file with ref count 1 created during index creation on replica shards
            // TODO: https://github.com/opensearch-project/OpenSearch/issues/17534
            if (localFile.startsWith(SEGMENTS)) {
                fileCache.remove(getFilePath(localFile));
            }
        }
        localDirectory.close();
    }

    @Override
    public String toString() {
        return "Composite Directory @ " + Integer.toHexString(hashCode());
    }

    /**
     * Function to perform operations once files have been uploaded to Remote Store
     * Currently deleting the local files here, as once uploaded to Remote, local files become eligible for eviction from FileCache
     * @param file : recent files which have been successfully uploaded to Remote Store
     */
    public void afterSyncToRemote(String file) {
        ensureOpen();

        logger.trace(
            "Composite Directory[{}]: File {} uploaded to Remote Store and now can be eligible for eviction in FileCache",
            this::toString,
            () -> file
        );
        final Path filePath = getFilePath(file);
        fileCache.unpin(filePath);
        // fileCache.remove(filePath);
    }

    // Visibility public since we need it in IT tests
    public Path getFilePath(String name) {
        return getLocalFSDirectory().getDirectory().resolve(name);
    }

    private FSDirectory getLocalFSDirectory() {
        FSDirectory localFSDirectory;
        if (localDirectory instanceof FSDirectory) {
            localFSDirectory = (FSDirectory) localDirectory;
        } else {
            // In this case it should be a FilterDirectory wrapped over FSDirectory as per above validation.
            localFSDirectory = (FSDirectory) (((FilterDirectory) localDirectory).getDelegate());
        }

        return localFSDirectory;
    }

    /**
     * Basic validations for Composite Directory parameters (null checks and instance type checks)
     *
     * Note: Currently Composite Directory only supports local directory to be of type FSDirectory
     * The reason is that FileCache currently has it key type as Path
     * Composite Directory currently uses FSDirectory's getDirectory() method to fetch and use the Path for operating on FileCache
     * TODO : Refactor FileCache to have key in form of String instead of Path. Once that is done we can remove this assertion
     */
    private void validate(Directory localDirectory, Directory remoteDirectory, FileCache fileCache) {
        if (localDirectory == null || remoteDirectory == null) throw new IllegalStateException(
            "Local and remote directory cannot be null for Composite Directory"
        );
        if (fileCache == null) throw new IllegalStateException(
            "File Cache not initialized on this Node, cannot create Composite Directory without FileCache"
        );
        if (localDirectory instanceof FSDirectory == false
            && !(localDirectory instanceof FilterDirectory && ((FilterDirectory) localDirectory).getDelegate() instanceof FSDirectory))
            throw new IllegalStateException("For Composite Directory, local directory must be of type FSDirectory");
        if (remoteDirectory instanceof RemoteSegmentStoreDirectory == false) throw new IllegalStateException(
            "For Composite Directory, remote directory must be of type RemoteSegmentStoreDirectory"
        );
    }

    /**
     * Return the list of files present in Remote
     */
    private String[] getRemoteFiles() throws IOException {
        String[] remoteFiles;
        try {
            remoteFiles = remoteDirectory.listAll();
        } catch (NullPointerException e) {
            /*
            We can encounter NPE when no data has been uploaded to remote store yet and as a result the metadata is empty
            Empty metadata means that there are no files currently in remote, hence returning an empty list in this scenario
            TODO : Catch the NPE in listAll of RemoteSegmentStoreDirectory itself instead of catching here
             */
            remoteFiles = new String[0];
        }
        return remoteFiles;
    }

    protected void cacheFile(String name) throws IOException {
        Path filePath = getFilePath(name);

        fileCache.put(filePath, new CachedFullFileIndexInput(fileCache, filePath, localDirectory.openInput(name, IOContext.DEFAULT)));
        fileCache.pin(filePath);
        fileCache.decRef(filePath);
    }

}
