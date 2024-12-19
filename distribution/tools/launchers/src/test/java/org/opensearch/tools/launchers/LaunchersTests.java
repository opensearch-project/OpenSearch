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

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LaunchersTests extends LaunchersTestCase {

    private Path tempDirectory;

    @After
    public void tearDown() throws IOException {
        if (tempDirectory != null && Files.exists(tempDirectory)) {
            Files.deleteIfExists(tempDirectory);
        }
    }

    @Test
    public void testCreateTempDirectory() throws IOException {
        String tempDirPrefix = "opensearch-test-" + RandomizedTest.randomAsciiLettersOfLength(7);
        tempDirectory = Launchers.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), tempDirPrefix);

        assertNotNull("Temporary directory should not be null", tempDirectory);
        assertTrue("Temporary directory should exist", Files.exists(tempDirectory));
    }
}
