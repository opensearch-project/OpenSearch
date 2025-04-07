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

package org.opensearch.bootstrap;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

public class SecurityTests extends OpenSearchTestCase {

    public void testEnsureExists() throws IOException {
        Path p = createTempDir();

        // directory exists
        Path exists = p.resolve("exists");
        Files.createDirectory(exists);
        Security.ensureDirectoryExists(exists);
        Files.createTempFile(exists, null, null);
    }

    public void testEnsureNotExists() throws IOException {
        Path p = createTempDir();

        // directory does not exist: create it
        Path notExists = p.resolve("notexists");
        Security.ensureDirectoryExists(notExists);
        Files.createTempFile(notExists, null, null);
    }

    public void testEnsureRegularFile() throws IOException {
        Path p = createTempDir();

        // regular file
        Path regularFile = p.resolve("regular");
        Files.createFile(regularFile);
        try {
            Security.ensureDirectoryExists(regularFile);
            fail("didn't get expected exception");
        } catch (IOException expected) {}
    }

    /** can't execute processes */
    @SuppressWarnings("removal")
    public void testProcessExecution() throws Exception {
        assumeTrue("test requires security manager", System.getSecurityManager() != null);
        try {
            Runtime.getRuntime().exec("ls");
            fail("didn't get expected exception");
        } catch (SecurityException expected) {}
    }

    @SuppressWarnings("removal")
    public void testReadPolicyWithCodebases() throws IOException {
        final Map<String, URL> codebases = Map.of(
            "test-netty-tcnative-boringssl-static-2.0.61.Final-linux-x86_64.jar",
            new URL("file://test-netty-tcnative-boringssl-static-2.0.61.Final-linux-x86_64.jar"),
            "test-kafka-server-common-3.6.1.jar",
            new URL("file://test-kafka-server-common-3.6.1.jar"),
            "test-kafka-server-common-3.6.1-test.jar",
            new URL("file://test-kafka-server-common-3.6.1-test.jar"),
            "test-lucene-core-9.11.0-snapshot-8a555eb.jar",
            new URL("file://test-lucene-core-9.11.0-snapshot-8a555eb.jar"),
            "test-zstd-jni-1.5.6-1.jar",
            new URL("file://test-zstd-jni-1.5.6-1.jar")
        );

        AccessController.doPrivileged(
            (PrivilegedAction<?>) () -> Security.readPolicy(SecurityTests.class.getResource("test-codebases.policy"), codebases)
        );
    }
}
