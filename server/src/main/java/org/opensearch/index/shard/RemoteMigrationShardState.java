/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

public class RemoteMigrationShardState {

    // Set to true for any primary shard started on remote backed node relocating from docrep node
    private final boolean shouldUpload;

    // Set to true for any primary shard started on remote backed node
    private final boolean remoteEnabled;

    public RemoteMigrationShardState(boolean shouldUpload, boolean remoteEnabled) {
        assert !this.shouldUpload() || (remoteEnabled == true);
        this.shouldUpload = shouldUpload;
        this.remoteEnabled = remoteEnabled;
    }

    public boolean shouldUpload() {
        return shouldUpload;
    }

    public boolean shouldDownload() {
        return remoteEnabled && shouldUpload == false;
    }

    public boolean isRemoteEnabled() {
        return remoteEnabled;
    }
}
