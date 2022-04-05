/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cli.Command;
import org.opensearch.cli.CommandTestCase;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
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
        Path home = fs.getPath("test-home");
        Path config = home.resolve("config");
        Files.createDirectories(config);
        Files.createFile(config.resolve("opensearch.yml"));
        Files.createDirectory(config.resolve("jvm.options.d"));
        Files.createFile(config.resolve("log4j2.properties"));
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
    public void testUpgrade() throws Exception {
        String passwd = "keystorepassword";

        Path esConfig = new File(getClass().getResource("/config").getPath()).toPath();
        // path for es_home
        terminal.addTextInput(esConfig.getParent().toString());
        // path for es_config
        terminal.addTextInput(esConfig.toString());
        // input for prompt 'config directory is inside installation'
        terminal.addTextInput("y");
        // input for prompt 'es version not detected'
        terminal.addTextInput("y");
        // input for prompt 'confirm the details look good'
        terminal.addTextInput("y");
        // as the keystore is password protected, we set it.
        terminal.addSecretInput(passwd);

        execute();

        assertYmlConfigImported();
        assertKeystoreImported(passwd);
        assertJvmOptionsImported();
        assertLog4jPropertiesImported();
    }

    private void assertYmlConfigImported() throws IOException {
        String[] headers = ImportYmlConfigTask.HEADER.split("[\\r\\n]+");
        List<String> expectedSettings = new ArrayList<>();
        expectedSettings.addAll(Arrays.asList(headers));
        // this is the generated flat settings
        expectedSettings.addAll(
            Arrays.asList(
                "---",
                "cluster.name: \"my-cluster\"",
                "http.port: \"42123\"",
                "node.name: \"node-x\"",
                "path.data:",
                "- \"/mnt/data_1\"",
                "- \"/mnt/data_2\"",
                "path.logs: \"/var/log/eslogs\""
            )
        );
        List<String> actualSettings = Files.readAllLines(env.configDir().resolve("opensearch.yml"))
            .stream()
            .filter(Objects::nonNull)
            .filter(line -> !line.isEmpty())
            .collect(Collectors.toList());

        assertThat(actualSettings, equalTo(expectedSettings));
    }

    private void assertKeystoreImported(String passwd) throws IOException, GeneralSecurityException {
        // assert keystore is created
        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configDir());
        assertNotNull(keystore);

        // assert all keystore settings are imported
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

    private void assertJvmOptionsImported() throws IOException, GeneralSecurityException {
        Path path = env.configDir().resolve("jvm.options.d");
        assertThat(Files.exists(path), is(true));
        assertThat(Files.isDirectory(path), is(true));
        assertThat(Files.exists(path.resolve("test.options")), is(true));
    }

    private void assertLog4jPropertiesImported() throws IOException, GeneralSecurityException {
        assertThat(Files.exists(env.configDir().resolve("log4j2.properties")), is(true));
    }
}
