/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.SpecialPermission;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class wraps the {@link ThreadContext} operations requiring access in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
@SuppressWarnings("removal")
public final class ThreadContextAccess {

    private ThreadContextAccess() {}

    public static <T> T doPrivileged(PrivilegedAction<T> operation) {
        SpecialPermission.check();
        return AccessController.doPrivileged(operation);
    }

    public static void doPrivilegedVoid(Runnable action) {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            action.run();
            return null;
        });
    }
}
