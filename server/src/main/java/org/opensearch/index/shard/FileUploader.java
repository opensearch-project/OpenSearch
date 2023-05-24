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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.CheckedFunction;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is a wrapper over the copying of file from local to remote store allowing to decorate the actual copy
 * method along with adding hooks of code that can be run before, on success and on failure.
 *
 * @opensearch.internal
 */
public class FileUploader {

    private static final Logger logger = LogManager.getLogger(FileUploader.class);

    private final UploadTracker uploadTracker;

    private final RemoteSegmentStoreDirectory remoteDirectory;

    private final Directory storeDirectory;

    private final Set<String> excludeFiles;

    private final CheckedFunction<String, String, IOException> checksumProvider;

    public FileUploader(
        UploadTracker uploadTracker,
        RemoteSegmentStoreDirectory remoteDirectory,
        Directory storeDirectory,
        Set<String> excludeFiles,
        CheckedFunction<String, String, IOException> checksumProvider
    ) {
        this.uploadTracker = uploadTracker;
        this.remoteDirectory = remoteDirectory;
        this.storeDirectory = storeDirectory;
        this.excludeFiles = excludeFiles;
        this.checksumProvider = checksumProvider;
    }

    /**
     * Calling this method will filter out files that need to be skipped and call
     * {@link RemoteSegmentStoreDirectory#copyFilesFrom}
     *
     * @param files The files that need to be uploaded
     * @return A boolean for whether all files were successful or not
     * @throws Exception when the underlying upload fails
     */
    public boolean uploadFiles(Collection<String> files) throws Exception {
        Collection<String> filteredFiles = files.stream().filter(file -> !skipUpload(file)).collect(Collectors.toList());
        return remoteDirectory.copyFilesFrom(storeDirectory, filteredFiles, IOContext.DEFAULT, uploadTracker);
    }

    /**
     * Whether to upload a file or not depending on whether file is in excluded list or has been already uploaded.
     *
     * @param file that needs to be uploaded.
     * @return true if the upload has to be skipped for the file.
     */
    private boolean skipUpload(String file) {
        try {
            // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
            return excludeFiles.contains(file) || remoteDirectory.containsFile(file, checksumProvider.apply(file));
        } catch (IOException e) {
            logger.error(
                "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                file
            );
        }
        return false;
    }
}
