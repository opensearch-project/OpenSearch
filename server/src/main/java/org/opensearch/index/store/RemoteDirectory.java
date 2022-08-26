/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A {@code RemoteDirectory} provides an abstraction layer for storing a list of files to a remote store.
 * A remoteDirectory contains only files (no sub-folder hierarchy). This class does not support all the methods in
 * the Directory interface. Currently, it contains implementation of methods which are used to copy files to/from
 * the remote store. Implementation of remaining methods will be added as remote store is integrated with
 * replication, peer recovery etc.
 *
 * @opensearch.internal
 */
public class RemoteDirectory extends Directory {

    private final BlobContainer blobContainer;

    public RemoteDirectory(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Returns names of all files stored in this directory. The output must be in sorted (UTF-16,
     * java's {@link String#compareTo}) order.
     */
    @Override
    public String[] listAll() throws IOException {
        return blobContainer.listBlobs().keySet().stream().sorted().toArray(String[]::new);
    }

    /**
     * Returns names of files with given prefix in this directory.
     * @param filenamePrefix The prefix to match against file names in the directory
     * @return A list of the matching filenames in the directory
     * @throws IOException if there were any failures in reading from the blob container
     */
    public Collection<String> listFilesByPrefix(String filenamePrefix) throws IOException {
        return blobContainer.listBlobsByPrefix(filenamePrefix).keySet();
    }

    /**
     * Removes an existing file in the directory.
     *
     * <p>This method will not throw an exception when the file doesn't exist and simply ignores this case.
     * This is a deviation from the {@code Directory} interface where it is expected to throw either
     * {@link NoSuchFileException} or {@link FileNotFoundException} if {@code name} points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException if the file exists but could not be deleted.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        // ToDo: Add a check for file existence
        blobContainer.deleteBlobsIgnoringIfNotExists(Collections.singletonList(name));
    }

    /**
     * Creates and returns a new instance of {@link RemoteIndexOutput} which will be used to copy files to the remote
     * store.
     *
     * <p> In the {@link Directory} interface, it is expected to throw {@link java.nio.file.FileAlreadyExistsException}
     * if the file already exists in the remote store. As this method does not open a file, it does not throw the
     * exception.
     *
     * @param name the name of the file to copy to remote store.
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return new RemoteIndexOutput(name, blobContainer);
    }

    /**
     * Opens a stream for reading an existing file and returns {@link RemoteIndexInput} enclosing the stream.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new RemoteIndexInput(name, blobContainer.readBlob(name), fileLength(name));
    }

    /**
     * Closes the remote directory. Currently, it is a no-op.
     * If remote directory maintains a state in future, we need to clean it before closing the directory
     */
    @Override
    public void close() throws IOException {
        // Do nothing
    }

    /**
     * Returns the byte length of a file in the directory.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public long fileLength(String name) throws IOException {
        // ToDo: Instead of calling remote store each time, keep a cache with segment metadata
        Map<String, BlobMetadata> metadata = blobContainer.listBlobsByPrefix(name);
        if (metadata.containsKey(name)) {
            return metadata.get(name).length();
        }
        throw new NoSuchFileException(name);
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Once soft deleting is supported segment files in the remote store, this method will provide details of
     * number of files marked as deleted but not actually deleted from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Set<String> getPendingDeletions() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Temporary IndexOutput is not required while working with Remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Segment upload to the remote store will be permanent and does not require a separate sync API.
     * This may change in the future if segment upload to remote store happens via cache and we need sync API to write
     * the cache contents to the store permanently.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Once metadata to be stored with each shard is finalized, syncMetaData method will be used to sync the directory
     * metadata to the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void syncMetaData() {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * As this method is used by IndexWriter to publish commits, the implementation of this method is required when
     * IndexWriter is backed by RemoteDirectory.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rename(String source, String dest) throws IOException {
        throw new UnsupportedOperationException();

    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Once locking segment files in remote store is supported, implementation of this method is required with
     * remote store specific LockFactory.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Lock obtainLock(String name) throws IOException {
        throw new UnsupportedOperationException();
    }
}
