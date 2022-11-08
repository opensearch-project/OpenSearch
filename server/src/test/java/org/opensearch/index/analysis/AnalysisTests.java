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

package org.opensearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class AnalysisTests extends OpenSearchTestCase {
    public void testParseStemExclusion() {
        /* Comma separated list */
        Settings settings = Settings.builder().put("stem_exclusion", "foo,bar").build();
        CharArraySet set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));

        /* Array */
        settings = Settings.builder().putList("stem_exclusion", "foo", "bar").build();
        set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));
    }

    public void testParseNonExistingFile() {
        Path tempDir = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("foo.bar_path", tempDir.resolve("foo.dict"))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Analysis.parseWordList(env, nodeSettings, "foo.bar", s -> s)
        );
        assertEquals("IOException while reading foo.bar_path: file not readable", ex.getMessage());
        assertNull(ex.getCause());
    }

    public void testParseFalseEncodedFile() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder().put("foo.bar_path", dict).put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        try (OutputStream writer = Files.newOutputStream(dict)) {
            writer.write(new byte[] { (byte) 0xff, 0x00, 0x00 }); // some invalid UTF-8
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Analysis.parseWordList(env, nodeSettings, "foo.bar", s -> s)
        );
        assertEquals("Unsupported character encoding detected while reading foo.bar_path: files must be UTF-8 encoded", ex.getMessage());
        assertNull(ex.getCause());
    }

    public void testParseWordList() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder().put("foo.bar_path", dict).put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("hello");
            writer.write('\n');
            writer.write("world");
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        List<String> wordList = Analysis.parseWordList(env, nodeSettings, "foo.bar", s -> s);
        assertEquals(Arrays.asList("hello", "world"), wordList);
    }

    public void testParseWordListError() throws IOException {
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Path dict = config.resolve("foo.dict");
        Settings nodeSettings = Settings.builder().put("foo.bar_path", dict).put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("abcd");
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        RuntimeException ex = expectThrows(
            RuntimeException.class,
            () -> Analysis.parseWordList(
                env,
                nodeSettings,
                "foo.bar",
                s -> { throw new RuntimeException("Error while parsing rule = " + s); }
            )
        );
        assertEquals("Line [1]: Error while parsing rule = abcd", ex.getMessage());
    }

    public void testParseWordListOutsideConfigDirError() throws IOException {
        Path home = createTempDir();
        Path temp = createTempDir();
        Path dict = temp.resolve("foo.dict");
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("abcd");
            writer.write('\n');
        }
        Settings nodeSettings = Settings.builder().put("foo.bar_path", dict).put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        RuntimeException ex = expectThrows(
            RuntimeException.class,
            () -> Analysis.parseWordList(
                env,
                nodeSettings,
                "foo.bar",
                s -> { throw new RuntimeException("Error while parsing rule = " + s); }
            )
        );
        assertEquals("Line [1]: Invalid rule", ex.getMessage());
    }
}
