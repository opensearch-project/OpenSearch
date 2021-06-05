/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cli.Command;
import org.opensearch.cli.CommandTestCase;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.upgrade.UpgradeCli;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

public class UpgradeCliTests extends CommandTestCase {
    private final List<FileSystem> fileSystems = new ArrayList<>();
    private Environment env;

    @Before
    public void setupEnv() throws IOException {
        final Configuration configuration;
        configuration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        FileSystem fs = Jimfs.newFileSystem(configuration);
        fileSystems.add(fs);
        PathUtilsForTesting.installMock(fs);
        Path home = fs.getPath("/", "test-home");
        Files.createDirectories(home.resolve("config"));
        Files.createFile(home.resolve("config/opensearch.yml"));
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
    }

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystems);
    }

    @Override
    protected Command newCommand() {
        return new UpgradeCli() {
            @Override
            protected Environment createEnv(Map<String, String> settings) {
                return env;
            }
        };
    }

    @SuppressForbidden(reason = "Read config directory from test resources.")
    public void testImportKeystoreSettings() throws Exception {
        String passwd = "keystorepassword";
        String esConfig = getClass().getResource("/config").getPath();
        // for testing we provide the input to the es config directory
        terminal.addTextInput(esConfig);
        // as the keystore is password protected, we set it.
        terminal.addSecretInput(passwd);
        execute();

        // assert settings are imported.
        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile());
        assertNotNull(keystore);
        keystore.decrypt(passwd.toCharArray());

        assertThat(keystore.getSettingNames(), hasItems(KeyStoreWrapper.SEED_SETTING.getKey(), "test.setting.key", "test.setting.file"));

        assertThat(keystore.getString("test.setting.key").toString(), is("test.setting.value"));

        InputStream is = keystore.getFile("test.setting.file");
        byte[] bytes = new byte[is.available()];
        assertThat(is.read(bytes), greaterThan(0));
        String actual = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).toString();
        String expected = "{\"some_key\": \"some_val\"}";
        assertThat(actual, is(expected));
    }
}
