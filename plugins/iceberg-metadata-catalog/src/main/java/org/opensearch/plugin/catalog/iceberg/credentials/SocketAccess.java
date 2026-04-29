/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg.credentials;

import org.opensearch.SpecialPermission;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Wraps AWS SDK calls that require the {@code SocketPermission "connect"} grant in the
 * plugin's security policy inside {@code AccessController.doPrivileged} blocks.
 * <p>
 * Mirrors {@code org.opensearch.repositories.s3.SocketAccess}.
 */
@SuppressWarnings("removal")
public final class SocketAccess {

    private SocketAccess() {}

    /** Runs the given action with the Security Manager bypass. */
    public static <T> T doPrivileged(PrivilegedAction<T> operation) {
        SpecialPermission.check();
        return AccessController.doPrivileged(operation);
    }

    /** Runs the given action with the Security Manager bypass; unwraps IOExceptions. */
    public static <T> T doPrivilegedIOException(PrivilegedExceptionAction<T> operation) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
