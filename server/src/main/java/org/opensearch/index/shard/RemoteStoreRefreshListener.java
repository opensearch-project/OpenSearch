/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    private final Directory storeDirectory;
    private final Directory remoteDirectory;
    // ToDo: This can be a map with metadata of the uploaded file as value of the map (GitHub #3398)
    private final Set<String> filesUploadedToRemoteStore;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(Directory storeDirectory, Directory remoteDirectory) throws IOException {
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        // ToDo: Handle failures in reading list of files (GitHub #3397)
        this.filesUploadedToRemoteStore = new HashSet<>(Arrays.asList(remoteDirectory.listAll()));
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do Nothing
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * The method also deletes segment files from remote store which are not part of local filesystem.
     * @param didRefresh true if the refresh opened a new reference
     * @throws IOException in case of I/O error in reading list of local files
     */
    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (didRefresh) {
            Set<String> localFiles = Arrays.stream(storeDirectory.listAll()).collect(Collectors.toSet());
            localFiles.stream().filter(file -> !filesUploadedToRemoteStore.contains(file)).forEach(file -> {
                try {
                    remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                    filesUploadedToRemoteStore.add(file);
                } catch (NoSuchFileException e) {
                    logger.info(
                        () -> new ParameterizedMessage("The file {} does not exist anymore. It can happen in case of temp files", file),
                        e
                    );
                } catch (IOException e) {
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file), e);
                }
            });

            Set<String> remoteFilesToBeDeleted = new HashSet<>();
            // ToDo: Instead of deleting files in sync, mark them and delete in async/periodic flow (GitHub #3142)
            filesUploadedToRemoteStore.stream().filter(file -> !localFiles.contains(file)).forEach(file -> {
                try {
                    remoteDirectory.deleteFile(file);
                    remoteFilesToBeDeleted.add(file);
                } catch (IOException e) {
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the remote segment store", file), e);
                }
            });

            remoteFilesToBeDeleted.forEach(filesUploadedToRemoteStore::remove);
        }
    }
}
