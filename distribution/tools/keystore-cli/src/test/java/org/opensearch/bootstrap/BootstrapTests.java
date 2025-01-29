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

import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tools.cli.keystore.KeyStoreCommandTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BootstrapTests extends OpenSearchTestCase {
    Environment env;
    List<FileSystem> fileSystems = new ArrayList<>();

    private static final int MAX_PASSPHRASE_LENGTH = 10;

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystems);
    }

    @Before
    public void setupEnv() throws IOException {
        env = KeyStoreCommandTestCase.setupEnv(true, fileSystems);
    }

    public void testLoadSecureSettings() throws Exception {
        final Path configPath = env.configDir();
        final SecureString seed;
        try (KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create()) {
            seed = KeyStoreWrapper.SEED_SETTING.get(Settings.builder().setSecureSettings(keyStoreWrapper).build());
            assertNotNull(seed);
            assertTrue(seed.length() > 0);
            keyStoreWrapper.save(configPath, new char[0]);
        }
        assertTrue(Files.exists(configPath.resolve("opensearch.keystore")));
        try (SecureSettings secureSettings = Bootstrap.loadSecureSettings(env)) {
            SecureString seedAfterLoad = KeyStoreWrapper.SEED_SETTING.get(Settings.builder().setSecureSettings(secureSettings).build());
            assertEquals(seedAfterLoad.toString(), seed.toString());
            assertTrue(Files.exists(configPath.resolve("opensearch.keystore")));
        }
    }

    public void testReadCharsFromStdin() throws Exception {
        assertPassphraseRead("hello", "hello");
        assertPassphraseRead("hello\n", "hello");
        assertPassphraseRead("hello\r\n", "hello");

        assertPassphraseRead("hellohello", "hellohello");
        assertPassphraseRead("hellohello\n", "hellohello");
        assertPassphraseRead("hellohello\r\n", "hellohello");

        assertPassphraseRead("hello\nhi\n", "hello");
        assertPassphraseRead("hello\r\nhi\r\n", "hello");
    }

    public void testPassphraseTooLong() throws Exception {
        byte[] source = "hellohello!\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(source)) {
            expectThrows(
                RuntimeException.class,
                "Password exceeded maximum length of 10",
                () -> Bootstrap.readPassphrase(stream, MAX_PASSPHRASE_LENGTH)
            );
        }
    }

    public void testNoPassPhraseProvided() throws Exception {
        byte[] source = "\r\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(source)) {
            expectThrows(
                RuntimeException.class,
                "Keystore passphrase required but none provided.",
                () -> Bootstrap.readPassphrase(stream, MAX_PASSPHRASE_LENGTH)
            );
        }
    }

    private void assertPassphraseRead(String source, String expected) {
        try (InputStream stream = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8))) {
            SecureString result = Bootstrap.readPassphrase(stream, MAX_PASSPHRASE_LENGTH);
            assertThat(result, equalTo(expected));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
