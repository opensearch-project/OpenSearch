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

package org.opensearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cli.Terminal;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.secure_sm.AccessController;

import java.io.IOError;

/**
 * UncaughtException Handler used during bootstrapping
 *
 * @opensearch.internal
 */
class OpenSearchUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LogManager.getLogger(OpenSearchUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread thread, Throwable t) {
        if (isFatalUncaught(t)) {
            try {
                onFatalUncaught(thread.getName(), t);
            } finally {
                // we use specific error codes in case the above notification failed, at least we
                // will have some indication of the error bringing us down
                if (t instanceof InternalError) {
                    halt(128);
                } else if (t instanceof OutOfMemoryError) {
                    halt(127);
                } else if (t instanceof StackOverflowError) {
                    halt(126);
                } else if (t instanceof UnknownError) {
                    halt(125);
                } else if (t instanceof IOError) {
                    halt(124);
                } else {
                    halt(1);
                }
            }
        } else {
            onNonFatalUncaught(thread.getName(), t);
        }
    }

    static boolean isFatalUncaught(Throwable e) {
        return e instanceof Error;
    }

    void onFatalUncaught(final String threadName, final Throwable t) {
        final String message = "fatal error in thread [" + threadName + "], exiting";
        logger.error(message, t);
        Terminal.DEFAULT.errorPrintln(message);
        t.printStackTrace(Terminal.DEFAULT.getErrorWriter());
        // Without a final flush, the stacktrace may not be shown before OpenSearch exits
        Terminal.DEFAULT.flush();
    }

    void onNonFatalUncaught(final String threadName, final Throwable t) {
        final String message = "uncaught exception in thread [" + threadName + "]";
        logger.error(message, t);
        Terminal.DEFAULT.errorPrintln(message);
        t.printStackTrace(Terminal.DEFAULT.getErrorWriter());
        // Without a final flush, the stacktrace may not be shown if OpenSearch goes on to exit
        Terminal.DEFAULT.flush();
    }

    void halt(int status) {
        AccessController.doPrivileged(new PrivilegedHaltAction(status));
    }

    static class PrivilegedHaltAction implements Runnable {

        private final int status;

        private PrivilegedHaltAction(final int status) {
            this.status = status;
        }

        @SuppressForbidden(reason = "halt")
        @Override
        public void run() {
            // we halt to prevent shutdown hooks from running
            Runtime.getRuntime().halt(status);
        }
    }
}
