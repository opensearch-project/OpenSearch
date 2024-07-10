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
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteReadResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract class that provides a base implementation for managing remote entities in the remote store.
 */
public abstract class AbstractRemoteWritableEntityManager implements RemoteWritableEntityManager {
    /**
     * A map that stores the remote writable entity stores, keyed by the entity type.
     */
    protected final Map<String, RemoteWritableEntityStore> remoteWritableEntityStores = new HashMap<>();

    /**
     * Retrieves the remote writable entity store for the given entity.
     *
     * @param entity the entity for which the store is requested
     * @return the remote writable entity store for the given entity
     * @throws IllegalArgumentException if the entity type is unknown
     */
    protected RemoteWritableEntityStore getStore(AbstractRemoteWritableBlobEntity entity) {
        RemoteWritableEntityStore remoteStore = remoteWritableEntityStores.get(entity.getType());
        if (remoteStore == null) {
            throw new IllegalArgumentException("Unknown entity type [" + entity.getType() + "]");
        }
        return remoteStore;
    }

    /**
     * Returns an ActionListener for handling the write operation for the specified component, remote object, and latched action listener.
     *
     * @param component the component for which the write operation is performed
     * @param remoteObject the remote object to be written
     * @param listener the listener to be notified when the write operation completes
     * @return an ActionListener for handling the write operation
     */
    protected abstract ActionListener<Void> getWriteActionListener(
        String component,
        AbstractRemoteWritableBlobEntity remoteObject,
        ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
    );

    /**
     * Returns an ActionListener for handling the read operation for the specified component,
     * remote object, and latched action listener.
     *
     * @param component the component for which the read operation is performed
     * @param remoteObject the remote object to be read
     * @param listener the listener to be notified when the read operation completes
     * @return an ActionListener for handling the read operation
     */
    protected abstract ActionListener<Object> getReadActionListener(
        String component,
        AbstractRemoteWritableBlobEntity remoteObject,
        ActionListener<RemoteReadResult> listener
    );

    @Override
    public CheckedRunnable<IOException> asyncWrite(
        String component,
        AbstractRemoteWritableBlobEntity entity,
        ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
    ) {
        return () -> getStore(entity).writeAsync(entity, getWriteActionListener(component, entity, listener));
    }

    @Override
    public CheckedRunnable<IOException> asyncRead(
        String component,
        AbstractRemoteWritableBlobEntity entity,
        ActionListener<RemoteReadResult> listener
    ) {
        return () -> getStore(entity).readAsync(entity, getReadActionListener(component, entity, listener));
    }
}
