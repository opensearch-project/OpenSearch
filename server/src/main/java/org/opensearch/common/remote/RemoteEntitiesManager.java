/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;

import java.io.IOException;

/**
 * The RemoteEntitiesManager interface provides async read and write methods for managing remote entities in the remote store
 */
public interface RemoteEntitiesManager {

    /**
     * Returns a CheckedRunnable that performs an asynchronous read operation for the specified component and entity.
     *
     * @param component the component for which the read operation is performed
     * @param entity the entity to be read
     * @param latchedActionListener the listener to be notified when the read operation completes
     * @return a CheckedRunnable that performs the asynchronous read operation
     */
    CheckedRunnable<IOException> getAsyncReadRunnable(
        String component,
        AbstractRemoteWritableBlobEntity entity,
        LatchedActionListener<RemoteReadResult> latchedActionListener
    );

    /**
     * Returns a CheckedRunnable that performs an asynchronous write operation for the specified component and entity.
     *
     * @param component the component for which the write operation is performed
     * @param entity the entity to be written
     * @param latchedActionListener the listener to be notified when the write operation completes
     * @return a CheckedRunnable that performs the asynchronous write operation
     */
    CheckedRunnable<IOException> getAsyncWriteRunnable(
        String component,
        AbstractRemoteWritableBlobEntity entity,
        LatchedActionListener<UploadedMetadata> latchedActionListener
    );
}
