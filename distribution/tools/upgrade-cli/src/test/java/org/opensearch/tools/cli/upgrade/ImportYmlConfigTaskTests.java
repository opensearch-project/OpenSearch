/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.opensearch.cli.MockTerminal;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class ImportYmlConfigTaskTests extends OpenSearchTestCase {
    private final MockTerminal terminal = new MockTerminal();
    private final List<FileSystem> fileSystems = new ArrayList<>();
    private ImportYmlConfigTask task;
    private Environment env;

    @Before
    public void setUpTask() throws IOException {
        task = new ImportYmlConfigTask();
        final Configuration configuration;
        configuration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        FileSystem fs = Jimfs.newFileSystem(configuration);
        fileSystems.add(fs);
        PathUtilsForTesting.installMock(fs);
        Path home = fs.getPath("test-home");
        Path config = home.resolve("config");
        Files.createDirectories(config);
        Files.createFile(config.resolve("opensearch.yml"));
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
    }

    @SuppressForbidden(reason = "Read config directory from test resources.")
    public void testImportYmlConfigTask() throws IOException {
        TaskInput taskInput = new TaskInput(env);
        Path esConfig = new File(getClass().getResource("/config").getPath()).toPath();
        taskInput.setEsConfig(esConfig);
        task.accept(new Tuple<>(taskInput, terminal));
        Settings settings = Settings.builder().loadFromPath(taskInput.getOpenSearchConfig().resolve("opensearch.yml")).build();
        assertThat(settings.keySet(), contains("cluster.name", "http.port", "node.name", "path.data", "path.logs"));
        assertThat(settings.get("cluster.name"), is("my-cluster"));
        assertThat(settings.get("http.port"), is("42123"));
        assertThat(settings.get("node.name"), is("node-x"));
        assertThat(settings.get("path.data"), is("[/mnt/data_1, /mnt/data_2]"));
        assertThat(settings.get("path.logs"), is("/var/log/eslogs"));
    }

    public void testMergeSettings() {
        Settings first = Settings.builder().put("setting_one", "value_one").build();
        Settings second = Settings.builder().put("setting_two", "value_two").build();
        Settings merged = task.mergeSettings(first, second);
        assertThat(merged.keySet(), contains("setting_one", "setting_two"));
        assertThat(merged.get("setting_one"), is("value_one"));
        assertThat(merged.get("setting_two"), is("value_two"));
    }
}
