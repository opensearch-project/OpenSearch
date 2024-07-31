/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm;

import java.security.BasicPermission;

/**
 * Permission to utilize methods in the ThreadContext class that are normally not accessible
 *
 * @see ThreadGroup
 * @see SecureSM
 */
public final class ThreadContextPermission extends BasicPermission {

    /**
     * Creates a new ThreadContextPermission object.
     *
     * @param name target name
     */
    public ThreadContextPermission(String name) {
        super(name);
    }

    /**
     * Creates a new ThreadContextPermission object.
     * This constructor exists for use by the {@code Policy} object to instantiate new Permission objects.
     *
     * @param name target name
     * @param actions ignored
     */
    public ThreadContextPermission(String name, String actions) {
        super(name, actions);
    }
}
