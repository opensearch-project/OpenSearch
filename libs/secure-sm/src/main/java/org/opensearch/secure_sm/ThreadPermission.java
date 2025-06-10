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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.secure_sm;

import java.security.BasicPermission;

/**
 * Permission to modify threads or thread groups normally not accessible
 * to the current thread.
 * <p>
 * In some cases (e.g. test runners), code needs to manipulate arbitrary threads,
 * so this Permission provides for that: the targets {@code modifyArbitraryThread} and
 * {@code modifyArbitraryThreadGroup} allow a thread blanket access to any group.
 *
 * @see ThreadGroup
 */
public final class ThreadPermission extends BasicPermission {

    /**
     * Creates a new ThreadPermission object.
     *
     * @param name target name
     */
    public ThreadPermission(String name) {
        super(name);
    }

    /**
     * Creates a new ThreadPermission object.
     * This constructor exists for use by the {@code Policy} object to instantiate new Permission objects.
     *
     * @param name target name
     * @param actions ignored
     */
    public ThreadPermission(String name, String actions) {
        super(name, actions);
    }
}
