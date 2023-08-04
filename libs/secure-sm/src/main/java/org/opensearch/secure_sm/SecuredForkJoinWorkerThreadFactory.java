/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permission;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;

public class SecuredForkJoinWorkerThreadFactory implements ForkJoinWorkerThreadFactory {
    static AccessControlContext contextWithPermissions(Permission... perms) {
        Permissions permissions = new Permissions();
        for (Permission perm : perms)
            permissions.add(perm);
        return new AccessControlContext(new ProtectionDomain[] { new ProtectionDomain(null, permissions) });
    }

    // ACC for access to the factory
    private static final AccessControlContext ACC = contextWithPermissions(
        new RuntimePermission("getClassLoader"),
        new RuntimePermission("setContextClassLoader"),
        new RuntimePermission("modifyThreadGroup"),
        new RuntimePermission("modifyThread")
    );

    public final ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return AccessController.doPrivileged(new PrivilegedAction<>() {
            public ForkJoinWorkerThread run() {
                return new ForkJoinWorkerThread(pool) {

                };
            }
        }, ACC);
    }
}
