/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;

import java.io.IOException;

/**
 * The RemoteWritableEntityManager interface provides async read and write methods for managing remote entities in the remote store
 */
public interface RemoteWritableEntityManager {

    /**
     * Returns a CheckedRunnable that performs an asynchronous read operation for the specified component and entity.
     *
     * @param component the component for which the read operation is performed
     * @param entity the entity to be read
     * @param listener the listener to be notified when the read operation completes
     * @return a CheckedRunnable that performs the asynchronous read operation
     */
    CheckedRunnable<IOException> asyncRead(
        String component,
        AbstractRemoteWritableBlobEntity entity,
        ActionListener<RemoteReadResult> listener
    );

    /**
     * Returns a CheckedRunnable that performs an asynchronous write operation for the specified component and entity.
     *
     * @param component the component for which the write operation is performed
     * @param entity the entity to be written
     * @param listener the listener to be notified when the write operation completes
     * @return a CheckedRunnable that performs the asynchronous write operation
     */
    CheckedRunnable<IOException> asyncWrite(
        String component,
        AbstractRemoteWritableBlobEntity entity,
        ActionListener<UploadedMetadata> listener
    );
}
