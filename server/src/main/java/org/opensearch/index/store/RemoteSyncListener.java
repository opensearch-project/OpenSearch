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
 * Listener that receives notifications after files are synced to the remote store.
 *
 * <p>Registered via {@code RemoteStoreUploaderService.addSyncListener()} at uploader
 * construction time. When a file is uploaded to the remote segment store, the uploader
 * calls {@link #afterSyncToRemote(String)} on all registered listeners.
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
@FunctionalInterface
@ExperimentalApi
public interface RemoteSyncListener {

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
