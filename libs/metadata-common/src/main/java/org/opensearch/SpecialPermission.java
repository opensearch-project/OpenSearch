/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch;

import java.security.BasicPermission;

/**
 * OpenSearch-specific permission to check before entering
 * {@code AccessController.doPrivileged()} blocks.
 * <p>
 * We try to avoid these blocks in our code and keep security simple,
 * but we need them for a few special places to contain hacks for third
 * party code, or dangerous things used by scripting engines.
 * <p>
 * All normal code has this permission, but checking this before truncating the stack
 * prevents unprivileged code (e.g. scripts), which do not have it, from gaining elevated
 * privileges.
 * <p>
 * In other words, don't do this:
 * <br>
 * <pre><code>
 *   // throw away all information about caller and run with our own privs
 *   AccessController.doPrivileged(
 *    ...
 *   );
 * </code></pre>
 * <br>
 * Instead do this;
 * <br>
 * <pre><code>
 *   // check caller first, to see if they should be allowed to do this
 *   SecurityManager sm = System.getSecurityManager();
 *   if (sm != null) {
 *     sm.checkPermission(new SpecialPermission());
 *   }
 *   // throw away all information about caller and run with our own privs
 *   AccessController.doPrivileged(
 *    ...
 *   );
 * </code></pre>
 *
 * @opensearch.internal
 */
public final class SpecialPermission extends BasicPermission {

    public static final SpecialPermission INSTANCE = new SpecialPermission();

    /**
     * Creates a new SpecialPermision object.
     */
    public SpecialPermission() {
        // TODO: if we really need we can break out name (e.g. "hack" or "scriptEngineService" or whatever).
        // but let's just keep it simple if we can.
        super("*");
    }

    /**
     * Creates a new SpecialPermission object.
     * This constructor exists for use by the {@code Policy} object to instantiate new Permission objects.
     *
     * @param name ignored
     * @param actions ignored
     */
    public SpecialPermission(String name, String actions) {
        this();
    }

    /**
     * Check that the current stack has {@link SpecialPermission} access according to the {@link SecurityManager}.
     */
    @SuppressWarnings("removal")
    public static void check() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(INSTANCE);
        }
    }
}
