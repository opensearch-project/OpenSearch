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

package org.opensearch.plugins;

import com.fasterxml.jackson.core.JsonParseException;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.ByteBufferStreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.semver.SemverRange;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PluginInfoTests extends OpenSearchTestCase {

    public void testReadFromProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertEquals(Version.CURRENT.toString(), info.getOpenSearchVersionRanges().get(0).toString());
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testReadFromPropertiesWithSingleOpenSearchRange() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"~" + Version.CURRENT.toString() + "\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertEquals("~" + Version.CURRENT.toString(), info.getOpenSearchVersionRanges().get(0).toString());
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testReadFromPropertiesWithFolderNameAndVersionAfter() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin",
            "custom.foldername",
            "custom-folder"
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertEquals("custom-folder", info.getTargetFolderName());
        assertEquals(Version.CURRENT.toString(), info.getOpenSearchVersionRanges().get(0).toString());
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [name] is missing in"));

        PluginTestUtil.writePluginProperties(pluginDir, "name", "");
        e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [name] is missing in"));
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir, "name", "fake-plugin");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[description] is missing"));
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir, "description", "fake desc", "name", "fake-plugin");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[version] is missing"));
    }

    public void testReadFromPropertiesOpenSearchVersionAndDependenciesMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir, "description", "fake desc", "name", "my_plugin", "version", "1.0");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(
            e.getMessage(),
            containsString("Either [opensearch.version] or [dependencies] property must be specified for the plugin ")
        );
    }

    public void testReadFromPropertiesWithDependenciesAndOpenSearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "dependencies",
            "{opensearch:" + Version.CURRENT.toString() + "}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(
            e.getMessage(),
            containsString("Only one of [opensearch.version] or [dependencies] property can be specified for the plugin")
        );
    }

    public void testReadFromPropertiesJavaVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "opensearch.version",
            Version.CURRENT.toString(),
            "version",
            "1.0"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[java.version] is missing"));
    }

    public void testReadFromPropertiesBadJavaVersionFormat() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            pluginName,
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            "1.7.0_80",
            "classname",
            "FakePlugin",
            "version",
            "1.0"
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(
            e.getMessage(),
            equalTo(
                "version string must be a sequence of nonnegative decimal integers separated"
                    + " by \".\"'s and may have leading zeros but was 1.7.0_80"
            )
        );
    }

    public void testReadFromPropertiesBogusOpenSearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "version",
            "1.0",
            "name",
            "my_plugin",
            "opensearch.version",
            "bogus"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("version needs to contain major, minor, and revision"));
    }

    public void testReadFromPropertiesJvmMissingClassname() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version")
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [classname] is missing"));
    }

    public void testExtendedPluginsSingleOptionalExtension() throws IOException {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin",
            "extended.plugins",
            "foo;optional=true"
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), contains("foo"));
        assertThat(info.isExtendedPluginOptional("foo"), is(true));
    }

    public void testExtendedPluginsSingleExtension() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin",
            "extended.plugins",
            "foo"
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), contains("foo"));
        assertThat(info.isExtendedPluginOptional("foo"), is(false));
    }

    public void testExtendedPluginsMultipleExtensions() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin",
            "extended.plugins",
            "foo,bar,baz"
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), contains("foo", "bar", "baz"));
    }

    public void testExtendedPluginsEmpty() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin",
            "extended.plugins",
            ""
        );
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testSerialize() throws Exception {
        PluginInfo info = new PluginInfo(
            "c",
            "foo",
            "dummy",
            Version.CURRENT,
            "1.8",
            "dummyclass",
            "c",
            Collections.singletonList("foo"),
            randomBoolean()
        );
        BytesStreamOutput output = new BytesStreamOutput();
        info.writeTo(output);
        ByteBuffer buffer = ByteBuffer.wrap(output.bytes().toBytesRef().bytes);
        ByteBufferStreamInput input = new ByteBufferStreamInput(buffer);
        PluginInfo info2 = new PluginInfo(input);
        assertThat(info2.toString(), equalTo(info.toString()));
    }

    public void testToXContent() throws Exception {
        PluginInfo info = new PluginInfo(
            "fake",
            "foo",
            "dummy",
            Version.CURRENT,
            "1.8",
            "dummyClass",
            "folder",
            Collections.emptyList(),
            false
        );
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        String prettyPrint = info.toXContent(builder, ToXContent.EMPTY_PARAMS).prettyPrint().toString();
        assertTrue(prettyPrint.contains("\"name\" : \"fake\""));
        assertTrue(prettyPrint.contains("\"version\" : \"dummy\""));
        assertTrue(prettyPrint.contains("\"opensearch_version\" : \"" + Version.CURRENT));
        assertTrue(prettyPrint.contains("\"java_version\" : \"1.8\""));
        assertTrue(prettyPrint.contains("\"description\" : \"foo\""));
        assertTrue(prettyPrint.contains("\"classname\" : \"dummyClass\""));
        assertTrue(prettyPrint.contains("\"custom_foldername\" : \"folder\""));
        assertTrue(prettyPrint.contains("\"extended_plugins\" : [ ]"));
        assertTrue(prettyPrint.contains("\"has_native_controller\" : false"));
    }

    public void testPluginListSorted() {
        List<PluginInfo> plugins = new ArrayList<>();
        plugins.add(new PluginInfo("c", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass", Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("b", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass", Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("e", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass", Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("a", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass", Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("d", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass", Collections.emptyList(), randomBoolean()));
        PluginsAndModules pluginsInfo = new PluginsAndModules(plugins, Collections.emptyList());

        final List<PluginInfo> infos = pluginsInfo.getPluginInfos();
        List<String> names = infos.stream().map(PluginInfo::getName).collect(Collectors.toList());
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }

    public void testUnknownProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "extra",
            "property",
            "unknown",
            "property",
            "description",
            "fake desc",
            "classname",
            "Foo",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version")
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Unknown properties in plugin descriptor"));
    }

    public void testMultipleDependencies() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"~" + Version.CURRENT.toString() + "\", dependency2:\"1.0.0\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Exactly one dependency is allowed to be specified in plugin descriptor properties"));
    }

    public void testNonOpenSearchDependency() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{some_dependency:\"~" + Version.CURRENT.toString() + "\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Only opensearch is allowed to be specified as a plugin dependency"));
    }

    public void testEmptyDependenciesProperty() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Exactly one dependency is allowed to be specified in plugin descriptor properties"));
    }

    public void testInvalidDependenciesProperty() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{invalid}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        expectThrows(JsonParseException.class, () -> PluginInfo.readFromProperties(pluginDir));
    }

    public void testEmptyOpenSearchVersionInDependencies() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Version cannot be empty"));
    }

    public void testInvalidOpenSearchVersionInDependencies() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"1.2\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(
            e.getMessage(),
            containsString("the version needs to contain major, minor, and revision, and optionally the build: 1.2")
        );
    }

    public void testInvalidRangeInDependencies() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"<2.2.0\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        expectThrows(NumberFormatException.class, () -> PluginInfo.readFromProperties(pluginDir));
    }

    public void testhMultipleOpenSearchRangesInDependencies() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "fake desc",
            "name",
            "my_plugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"~1.2.3, =1.2.3\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakePlugin"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Exactly one range is allowed to be specified in dependencies for the plugin"));
    }

    public void testhMultipleOpenSearchRangesInConstructor() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PluginInfo(
                "plugin_name",
                "foo",
                "dummy",
                List.of(
                    new SemverRange(Version.CURRENT, SemverRange.RangeOperator.EQ),
                    new SemverRange(Version.CURRENT, SemverRange.RangeOperator.DEFAULT)
                ),
                "1.8",
                "dummyclass",
                null,
                Collections.emptyList(),
                randomBoolean()
            )
        );
        assertThat(e.getMessage(), containsString("Exactly one range is allowed to be specified in dependencies for the plugin"));
    }
}
