/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * Interface to handle the functionality for uploading data in the remote store
 */
public interface RemoteStoreUploader {

    /**
     * Upload segments using FileMetadata for format-aware routing
     *
     * @param fileMetadataCollection collection of FileMetadata objects containing format and file information
     * @param fileMeatadataSizeMap map of segment file names to their sizes
     * @param listener listener to be notified when upload completes
     * @param uploadListenerFunction function to create upload listeners
     * @param isLowPriorityUpload whether this is a low priority upload
     */
    void uploadSegments(
        Collection<FileMetadata> fileMetadataCollection,
        Map<FileMetadata, Long> fileMeatadataSizeMap,
        ActionListener<Void> listener,
        Function<Map<FileMetadata, Long>, UploadListener> uploadListenerFunction,
        boolean isLowPriorityUpload
    );
}
