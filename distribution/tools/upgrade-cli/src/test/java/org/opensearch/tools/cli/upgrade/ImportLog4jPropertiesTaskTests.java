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
import java.util.Properties;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ImportLog4jPropertiesTaskTests extends OpenSearchTestCase {
    private final MockTerminal terminal = new MockTerminal();
    private final List<FileSystem> fileSystems = new ArrayList<>();
    private ImportLog4jPropertiesTask task;
    private Environment env;

    @Before
    public void setUpTask() throws IOException {
        task = new ImportLog4jPropertiesTask();
        final Configuration configuration;
        configuration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        FileSystem fs = Jimfs.newFileSystem(configuration);
        fileSystems.add(fs);
        PathUtilsForTesting.installMock(fs);
        Path home = fs.getPath("test-home");
        Path config = home.resolve("config");
        Files.createDirectories(config);
        Files.createFile(config.resolve(ImportLog4jPropertiesTask.LOG4J_PROPERTIES));
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
    }

    @SuppressForbidden(reason = "Read config directory from test resources.")
    public void testImportLog4jPropertiesTask() throws IOException {
        TaskInput taskInput = new TaskInput(env);
        Path esConfig = new File(getClass().getResource("/config").getPath()).toPath();
        taskInput.setEsConfig(esConfig);
        task.accept(new Tuple<>(taskInput, terminal));

        Properties properties = new Properties();
        properties.load(Files.newInputStream(taskInput.getOpenSearchConfig().resolve(ImportLog4jPropertiesTask.LOG4J_PROPERTIES)));
        assertThat(properties, is(notNullValue()));
        assertThat(properties.entrySet(), hasSize(165));
        assertThat(properties.get("appender.rolling.layout.type"), equalTo("OpenSearchJsonLayout"));
        assertThat(
            properties.get("appender.deprecation_rolling.fileName"),
            equalTo("${sys:opensearch.logs.base_path}${sys:file.separator}${sys:opensearch.logs.cluster_name}_deprecation.json")
        );
        assertThat(properties.get("logger.deprecation.name"), equalTo("org.opensearch.deprecation"));
        assertThat(properties.keySet(), not(hasItem("appender.deprecation_rolling.layout.esmessagefields")));
        assertThat(properties.keySet(), hasItem("appender.deprecation_rolling.layout.opensearchmessagefields"));
    }
}
