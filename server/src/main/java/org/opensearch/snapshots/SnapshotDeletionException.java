/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.rest.RestStatus;

public class SnapshotDeletionException extends SnapshotException {

    public SnapshotDeletionException(final String repositoryName, final String snapshotName, final String msg) {
        super(repositoryName, snapshotName, msg);
    }

    @Override
    public RestStatus status() {
        return RestStatus.FORBIDDEN;
    }
}
