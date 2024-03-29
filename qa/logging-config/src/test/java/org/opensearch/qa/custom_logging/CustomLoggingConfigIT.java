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

package org.opensearch.qa.custom_logging;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.hamcrest.RegexMatcher;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

/**
 * This test verifies that OpenSearch can startup successfully with a custom logging config using variables introduced in
 * <code>OpenSearchJsonLayout</code>
 * The intention is to confirm that users can still run their OpenSearch instances with previous configurations.
 */
public class CustomLoggingConfigIT extends OpenSearchRestTestCase {
    private static final String NODE_STARTED = ".*integTest-0.*cluster.uuid.*node.id.*recovered.*cluster_state.*";

    public void testSuccessfulStartupWithCustomConfig() throws Exception {
        assertBusy(() -> {
            List<String> lines = readAllLines(getLogFile());
            assertThat(lines, Matchers.hasItem(RegexMatcher.matches(NODE_STARTED)));
        });
    }

    @SuppressWarnings("removal")
    private List<String> readAllLines(Path logFile) {
        return AccessController.doPrivileged((PrivilegedAction<List<String>>) () -> {
            try {
                return Files.readAllLines(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @SuppressForbidden(reason = "PathUtils doesn't have permission to read this file")
    private Path getLogFile() {
        String logFileString = System.getProperty("tests.logfile");
        if (logFileString == null) {
            fail("tests.logfile must be set to run this test. It is automatically "
                + "set by gradle. If you must set it yourself then it should be the absolute path to the "
                + "log file.");
        }
        return Paths.get(logFileString);
    }
}
