/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;

/**
 * The RemoteWritableEntityManager interface provides async read and write methods for managing remote entities in the remote store
 */
public interface RemoteWritableEntityManager {

    /**
     * Performs an asynchronous read operation for the specified component and entity.
     *
     * @param component the component for which the read operation is performed
     * @param entity the entity to be read
     * @param listener the listener to be notified when the read operation completes.
     *                 The listener's {@link ActionListener#onResponse(Object)} method
     *                 is called with a {@link RemoteReadResult} object containing the
     *                 read data on successful read. The
     *                 {@link ActionListener#onFailure(Exception)} method is called with
     *                 an exception if the read operation fails.
     */
    void readAsync(String component, AbstractClusterMetadataWriteableBlobEntity entity, ActionListener<RemoteReadResult> listener);

    /**
     * Performs an asynchronous write operation for the specified component and entity.
     *
     * @param component the component for which the write operation is performed
     * @param entity the entity to be written
     * @param listener the listener to be notified when the write operation completes.
     *                 The listener's {@link ActionListener#onResponse(Object)} method
     *                 is called with a {@link UploadedMetadata} object containing the
     *                 uploaded metadata on successful write. The
     *                 {@link ActionListener#onFailure(Exception)} method is called with
     *                 an exception if the write operation fails.
     */
    void writeAsync(String component, AbstractClusterMetadataWriteableBlobEntity entity, ActionListener<UploadedMetadata> listener);
}
