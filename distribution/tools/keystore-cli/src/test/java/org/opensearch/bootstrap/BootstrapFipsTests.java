/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.settings.SecureString;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class BootstrapFipsTests extends BootstrapTests {

    public void testLoadSecureSettings() throws Exception {
        final Path configPath = env.configDir();
        final SecureString seed;
        try (KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create()) {
            seed = KeyStoreWrapper.SEED_SETTING.get(Settings.builder().setSecureSettings(keyStoreWrapper).build());
            assertNotNull(seed);
            assertTrue(seed.length() > 0);
            keyStoreWrapper.save(configPath, "keystorepassword".toCharArray());
        }

        String passwordInput = new String("keystorepassword".toCharArray()) + System.lineSeparator();
        assertTrue(Files.exists(configPath.resolve("opensearch.keystore")));
        InputStream originalIn = System.in;
        try {
            System.setIn(new ByteArrayInputStream(passwordInput.getBytes(StandardCharsets.UTF_8)));
            try (SecureSettings secureSettings = Bootstrap.loadSecureSettings(env)) {
                SecureString seedAfterLoad = KeyStoreWrapper.SEED_SETTING.get(Settings.builder().setSecureSettings(secureSettings).build());
                assertEquals(seedAfterLoad.toString(), seed.toString());
                assertTrue(Files.exists(configPath.resolve("opensearch.keystore")));
            }
        } finally {
            System.setIn(originalIn);
        }
    }

}
