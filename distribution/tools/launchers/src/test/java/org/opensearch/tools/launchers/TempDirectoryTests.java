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

import org.opensearch.tools.java_version_checker.SuppressForbidden;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.mockito.MockedStatic;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

@SuppressForbidden(reason = "modifies system properties to test different O.S")
public class TempDirectoryTests extends LaunchersTestCase {

    private Path createdTempDir;

    @After
    public void tearDown() throws IOException {
        if (createdTempDir != null && Files.exists(createdTempDir)) {
            Files.delete(createdTempDir);
        }
    }

    @Test
    public void testMainWithArguments() {
        String[] args = { RandomizedTest.randomAsciiLettersOfLengthBetween(1, 10) };
        assertThrows(IllegalArgumentException.class, () -> TempDirectory.main(args));
    }

    @Test
    public void testMainOnWindows() throws IOException {
        System.setProperty("os.name", "Windows " + RandomizedTest.randomIntBetween(7, 10));

        try (MockedStatic<Launchers> mockedLaunchers = mockStatic(Launchers.class)) {
            mockedLaunchers.when(() -> Launchers.outPrintln(any(String.class))).thenAnswer(invocation -> null);

            TempDirectory.main(new String[] {});

            createdTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "opensearch");
            mockedLaunchers.verify(() -> Launchers.outPrintln(createdTempDir.toString()), times(1));
            assert Files.exists(createdTempDir);
        }
    }

    @Test
    public void testMainOnNonWindows() throws IOException {
        String[] osNames = { "Linux", "Mac OS X", "Unix" };
        String osName = RandomizedTest.randomFrom(osNames);
        System.setProperty("os.name", osName);

        try (MockedStatic<Launchers> mockedLaunchers = mockStatic(Launchers.class)) {
            Path mockedTempDir = Paths.get(
                System.getProperty("java.io.tmpdir"),
                "opensearch-" + RandomizedTest.randomAsciiLettersOfLength(7)
            );
            mockedLaunchers.when(() -> Launchers.createTempDirectory(any(Path.class), eq("opensearch-"))).thenReturn(mockedTempDir);
            mockedLaunchers.when(() -> Launchers.outPrintln(any(String.class))).thenAnswer(invocation -> null);

            TempDirectory.main(new String[] {});

            mockedLaunchers.verify(() -> Launchers.createTempDirectory(any(Path.class), eq("opensearch-")), times(1));
            mockedLaunchers.verify(() -> Launchers.outPrintln(mockedTempDir.toString()), times(1));
        }
    }
}
