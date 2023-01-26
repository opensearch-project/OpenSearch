/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.SpecialPermission;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.identity.IdentityPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotUtils;
import org.opensearch.threadpool.ThreadPool;

public class SnapshotRestoreHelper {

    protected static final Logger log = LogManager.getLogger(SnapshotRestoreHelper.class);

    public static List<String> resolveOriginalIndices(RestoreSnapshotRequest restoreRequest) {
        final SnapshotInfo snapshotInfo = getSnapshotInfo(restoreRequest);

        if (snapshotInfo == null) {
            log.warn("snapshot repository '{}', snapshot '{}' not found", restoreRequest.repository(), restoreRequest.snapshot());
            return null;
        } else {
            return SnapshotUtils.filterIndices(snapshotInfo.indices(), restoreRequest.indices(), restoreRequest.indicesOptions());
        }


    }

    public static SnapshotInfo getSnapshotInfo(RestoreSnapshotRequest restoreRequest) {
        final RepositoriesService repositoriesService = Objects.requireNonNull(IdentityPlugin.GuiceHolder.getRepositoriesService(), "RepositoriesService not initialized");
        final Repository repository = repositoriesService.repository(restoreRequest.repository());
        final String threadName = Thread.currentThread().getName();
        SnapshotInfo snapshotInfo = null;

        try {
            setCurrentThreadName("[" + ThreadPool.Names.GENERIC + "]");
            for (SnapshotId snapshotId : PlainActionFuture.get(repository::getRepositoryData).getSnapshotIds()) {
                if (snapshotId.getName().equals(restoreRequest.snapshot())) {

                    if(log.isDebugEnabled()) {
                        log.debug("snapshot found: {} (UUID: {})", snapshotId.getName(), snapshotId.getUUID());
                    }

                    snapshotInfo = repository.getSnapshotInfo(snapshotId);
                    break;
                }
            }
        } finally {
            setCurrentThreadName(threadName);
        }
        return snapshotInfo;
    }

    @SuppressWarnings("removal")
    private static void setCurrentThreadName(final String name) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                Thread.currentThread().setName(name);
                return null;
            }
        });
    }

}
