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

package org.opensearch.tools.launchers;

import org.opensearch.tools.java_version_checker.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

/**
 * Utility methods for launchers.
 */
final class Launchers {

    /**
     * Prints a string and terminates the line on standard output.
     *
     * @param message the message to print
     */
    @SuppressForbidden(reason = "System#out")
    public static void outPrintln(final String message) {
        System.out.println(message);
    }

    /**
     * Prints a string and terminates the line on standard error.
     *
     * @param message the message to print
     */
    @SuppressForbidden(reason = "System#err")
    public static void errPrintln(final String message) {
        System.err.println(message);
    }

    /**
     * Exit the VM with the specified status.
     *
     * @param status the status
     */
    @SuppressForbidden(reason = "System#exit")
    public static void exit(final int status) {
        System.exit(status);
    }

    @SuppressForbidden(reason = "Files#createTempDirectory(String, FileAttribute...)")
    static Path createTempDirectory(final Path rootDir, final String prefix, final FileAttribute<?>... attrs) throws IOException {
        return Files.createTempDirectory(rootDir, prefix, attrs);
    }
}
