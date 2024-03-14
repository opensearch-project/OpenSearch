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

package org.opensearch.repositories.gcs;

import org.apache.logging.log4j.core.util.Throwables;
import org.opensearch.SpecialPermission;
import org.opensearch.common.CheckedRunnable;

import java.io.IOException;
import java.net.SocketPermission;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This plugin uses google api/client libraries to connect to google cloud services. For these remote calls the plugin
 * needs {@link SocketPermission} 'connect' to establish connections. This class wraps the operations requiring access
 * in {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
@SuppressWarnings("removal")
final class SocketAccess {

    private SocketAccess() {}

    public static <T> T doPrivilegedIOException(PrivilegedExceptionAction<T> operation) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static void doPrivilegedVoidIOException(CheckedRunnable<IOException> action) throws IOException {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.run();
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            Throwables.rethrow(e.getCause());
            assert false : "always throws";
            return null;
        }
    }

}
