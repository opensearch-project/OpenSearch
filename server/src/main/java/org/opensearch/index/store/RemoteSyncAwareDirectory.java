/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Interface for directories that need notification after files are synced to the remote store.
 *
 * <p>When a file is uploaded to the remote segment store, the uploader service walks the
 * {@link org.apache.lucene.store.FilterDirectory} chain to find a directory implementing this
 * interface and calls {@link #afterSyncToRemote(String)}. This allows each directory layer to
 * react to the sync event — for example, unpinning a file from the local cache so it becomes
 * eligible for eviction, or updating a file registry to reflect the new remote location.
 *
 * <p>Implemented by:
 * <ul>
 *   <li>{@link CompositeDirectory} — unpins files from FileCache after upload</li>
 *   <li>TieredSubdirectoryAwareDirectory — delegates to format-specific handlers</li>
 *   <li>{@link DataFormatAwareStoreDirectory} — pass-through to inner directory</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface RemoteSyncAwareDirectory {

    /**
     * Called after a file has been successfully uploaded to the remote store.
     *
     * <p>Implementations should use this callback to update internal state related to the
     * file's remote availability — such as unpinning from a local cache, marking the file
     * as remotely available in a registry, or forwarding the notification to a delegate.
     *
     * @param file the name of the file that was synced to remote
     */
    void afterSyncToRemote(String file);
}
