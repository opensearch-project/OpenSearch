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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonReadFeature;

import org.opensearch.Version;
import org.opensearch.bootstrap.JarHell;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.semver.SemverRange;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An in-memory representation of the plugin descriptor.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PluginInfo implements Writeable, ToXContentObject {

    public static final String OPENSEARCH_PLUGIN_PROPERTIES = "plugin-descriptor.properties";
    public static final String OPENSEARCH_PLUGIN_POLICY = "plugin-security.policy";
    public static final String OPENSEARCH_PLUGIN_ACTIONS = "plugin-permissions.yml";
    private static final JsonFactory jsonFactory = new JsonFactory().configure(
        JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES.mappedFeature(),
        true
    );

    private final String name;
    private final String description;
    private final String version;
    private final List<SemverRange> opensearchVersionRanges;
    private final String javaVersion;
    private final String classname;
    private final String customFolderName;
    private final List<String> extendedPlugins;
    private final boolean hasNativeController;
    private final Settings requestedActions;

    /**
     * Construct plugin info.
     *
     * @param name                  the name of the plugin
     * @param description           a description of the plugin
     * @param version               an opaque version identifier for the plugin
     * @param opensearchVersion     the version of OpenSearch the plugin was built for
     * @param javaVersion           the version of Java the plugin was built with
     * @param classname             the entry point to the plugin
     * @param customFolderName      the custom folder name for the plugin
     * @param extendedPlugins       other plugins this plugin extends through SPI
     * @param hasNativeController   whether or not the plugin has a native controller
     */
    public PluginInfo(
        String name,
        String description,
        String version,
        Version opensearchVersion,
        String javaVersion,
        String classname,
        String customFolderName,
        List<String> extendedPlugins,
        boolean hasNativeController,
        Settings requestedActions
    ) {
        this(
            name,
            description,
            version,
            List.of(SemverRange.fromString(opensearchVersion.toString())),
            javaVersion,
            classname,
            customFolderName,
            extendedPlugins,
            hasNativeController,
            requestedActions
        );
    }

    public PluginInfo(
        String name,
        String description,
        String version,
        List<SemverRange> opensearchVersionRanges,
        String javaVersion,
        String classname,
        String customFolderName,
        List<String> extendedPlugins,
        boolean hasNativeController,
        Settings requestedActions
    ) {
        this.name = name;
        this.description = description;
        this.version = version;
        // Ensure only one range is specified (for now)
        if (opensearchVersionRanges.size() != 1) {
            throw new IllegalArgumentException(
                "Exactly one range is allowed to be specified in dependencies for the plugin [" + name + "]"
            );
        }
        this.opensearchVersionRanges = opensearchVersionRanges;
        this.javaVersion = javaVersion;
        this.classname = classname;
        this.customFolderName = customFolderName;
        this.extendedPlugins = Collections.unmodifiableList(extendedPlugins);
        this.hasNativeController = hasNativeController;
        this.requestedActions = requestedActions;
    }

    /**
     * Construct plugin info.
     *
     * @param name                  the name of the plugin
     * @param description           a description of the plugin
     * @param version               an opaque version identifier for the plugin
     * @param opensearchVersion     the version of OpenSearch the plugin was built for
     * @param javaVersion           the version of Java the plugin was built with
     * @param classname             the entry point to the plugin
     * @param extendedPlugins       other plugins this plugin extends through SPI
     * @param hasNativeController   whether or not the plugin has a native controller
     */
    public PluginInfo(
        String name,
        String description,
        String version,
        Version opensearchVersion,
        String javaVersion,
        String classname,
        List<String> extendedPlugins,
        boolean hasNativeController
    ) {
        this(
            name,
            description,
            version,
            opensearchVersion,
            javaVersion,
            classname,
            null /* customFolderName */,
            extendedPlugins,
            hasNativeController,
            Settings.EMPTY /* requestedActions */
        );
    }

    /**
     * Construct plugin info from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    @SuppressWarnings("unchecked")
    public PluginInfo(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.description = in.readString();
        this.version = in.readString();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            this.opensearchVersionRanges = (List<SemverRange>) in.readGenericValue();
        } else {
            this.opensearchVersionRanges = List.of(new SemverRange(in.readVersion(), SemverRange.RangeOperator.DEFAULT));
        }
        this.javaVersion = in.readString();
        this.classname = in.readString();
        this.customFolderName = in.readString();
        this.extendedPlugins = in.readStringList();
        this.hasNativeController = in.readBoolean();
        // TODO switch this to 2.X version this change will be released in after backport
        if (in.getVersion().onOrAfter(Version.CURRENT)) {
            this.requestedActions = Settings.readSettingsFromStream(in);
        } else {
            this.requestedActions = Settings.EMPTY;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeString(version);
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            out.writeGenericValue(opensearchVersionRanges);
        } else {
            /*
            This works for currently supported range notations (=,~)
            As more notations get added, then a suitable version must be picked.
             */
            out.writeVersion(opensearchVersionRanges.get(0).getRangeVersion());
        }
        out.writeString(javaVersion);
        out.writeString(classname);
        if (customFolderName != null) {
            out.writeString(customFolderName);
        } else {
            out.writeString(name);
        }
        out.writeStringCollection(extendedPlugins);
        out.writeBoolean(hasNativeController);
        if (out.getVersion().onOrAfter(Version.CURRENT)) {
            if (requestedActions != null) {
                Settings.writeSettingsToStream(requestedActions, out);
            } else {
                Settings.writeSettingsToStream(Settings.EMPTY, out);
            }
        }
    }

    /**
     * Reads the plugin descriptor file.
     *
     * @param path           the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(final Path path) throws IOException {
        final Path descriptor = path.resolve(OPENSEARCH_PLUGIN_PROPERTIES);

        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptor)) {
                props.load(stream);
            }
            propsMap = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        final String name = propsMap.remove("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("property [name] is missing in [" + descriptor + "]");
        }
        final String description = propsMap.remove("description");
        if (description == null) {
            throw new IllegalArgumentException("property [description] is missing for plugin [" + name + "]");
        }
        final String version = propsMap.remove("version");
        if (version == null) {
            throw new IllegalArgumentException("property [version] is missing for plugin [" + name + "]");
        }

        final String opensearchVersionString = propsMap.remove("opensearch.version");
        final String dependenciesValue = propsMap.remove("dependencies");
        if (opensearchVersionString == null && dependenciesValue == null) {
            throw new IllegalArgumentException(
                "Either [opensearch.version] or [dependencies] property must be specified for the plugin [" + name + "]"
            );
        }
        if (opensearchVersionString != null && dependenciesValue != null) {
            throw new IllegalArgumentException(
                "Only one of [opensearch.version] or [dependencies] property can be specified for the plugin [" + name + "]"
            );
        }

        final List<SemverRange> opensearchVersionRanges = new ArrayList<>();
        if (opensearchVersionString != null) {
            opensearchVersionRanges.add(SemverRange.fromString(opensearchVersionString));
        } else {
            Map<String, String> dependenciesMap;
            try (
                final JsonXContentParser parser = new JsonXContentParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.IGNORE_DEPRECATIONS,
                    jsonFactory.createParser(dependenciesValue)
                )
            ) {
                dependenciesMap = parser.mapStrings();
            }
            if (dependenciesMap.size() != 1) {
                throw new IllegalArgumentException(
                    "Exactly one dependency is allowed to be specified in plugin descriptor properties: " + dependenciesMap
                );
            }
            if (dependenciesMap.keySet().stream().noneMatch(s -> s.equals("opensearch"))) {
                throw new IllegalArgumentException("Only opensearch is allowed to be specified as a plugin dependency: " + dependenciesMap);
            }
            String[] ranges = dependenciesMap.get("opensearch").split(",");
            if (ranges.length != 1) {
                throw new IllegalArgumentException(
                    "Exactly one range is allowed to be specified in dependencies for the plugin [\" + name + \"]"
                );
            }
            opensearchVersionRanges.add(SemverRange.fromString(ranges[0].trim()));
        }

        final String javaVersionString = propsMap.remove("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException("property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);
        final String classname = propsMap.remove("classname");
        if (classname == null) {
            throw new IllegalArgumentException("property [classname] is missing for plugin [" + name + "]");
        }

        final String customFolderNameValue = propsMap.remove("custom.foldername");
        final String customFolderName;
        customFolderName = customFolderNameValue;

        final String extendedString = propsMap.remove("extended.plugins");
        final List<String> extendedPlugins;
        if (extendedString == null) {
            extendedPlugins = Collections.emptyList();
        } else {
            extendedPlugins = Arrays.asList(Strings.delimitedListToStringArray(extendedString, ","));
        }

        final String hasNativeControllerValue = propsMap.remove("has.native.controller");
        final boolean hasNativeController;
        if (hasNativeControllerValue == null) {
            hasNativeController = false;
        } else {
            switch (hasNativeControllerValue) {
                case "true":
                    hasNativeController = true;
                    break;
                case "false":
                    hasNativeController = false;
                    break;
                default:
                    final String message = String.format(
                        Locale.ROOT,
                        "property [%s] must be [%s], [%s], or unspecified but was [%s]",
                        "has_native_controller",
                        "true",
                        "false",
                        hasNativeControllerValue
                    );
                    throw new IllegalArgumentException(message);
            }
        }

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties in plugin descriptor: " + propsMap.keySet());
        }

        Settings requestedActions = Settings.EMPTY;
        Path actions = path.resolve(PluginInfo.OPENSEARCH_PLUGIN_ACTIONS);

        if (Files.exists(actions)) {
            try {
                requestedActions = PluginSecurity.parseRequestedActions(actions);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new PluginInfo(
            name,
            description,
            version,
            opensearchVersionRanges,
            javaVersionString,
            classname,
            customFolderName,
            extendedPlugins,
            hasNativeController,
            requestedActions
        );
    }

    /**
     * The name of the plugin.
     *
     * @return the plugin name
     */
    public String getName() {
        return name;
    }

    /**
     * The description of the plugin.
     *
     * @return the plugin description
     */
    public String getDescription() {
        return description;
    }

    /**
     * The entry point to the plugin.
     *
     * @return the entry point to the plugin
     */
    public String getClassname() {
        return classname;
    }

    /**
     * The custom folder name for the plugin.
     *
     * @return the custom folder name for the plugin
     */
    public String getFolderName() {
        return customFolderName;
    }

    /**
     * Other plugins this plugin extends through SPI.
     *
     * @return the names of the plugins extended
     */
    public List<String> getExtendedPlugins() {
        return extendedPlugins;
    }

    /**
     * The version of the plugin
     *
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * The list of OpenSearch version ranges the plugin is compatible with.
     *
     * @return a list of OpenSearch version ranges
     */
    public List<SemverRange> getOpenSearchVersionRanges() {
        return opensearchVersionRanges;
    }

    /**
     * Pretty print the semver ranges and return the string.
     * @return semver ranges string
     */
    public String getOpenSearchVersionRangesString() {
        if (opensearchVersionRanges == null || opensearchVersionRanges.isEmpty()) {
            throw new IllegalStateException("Opensearch version ranges list cannot be empty");
        }
        if (opensearchVersionRanges.size() == 1) {
            return opensearchVersionRanges.get(0).toString();
        }
        return opensearchVersionRanges.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * The version of Java the plugin was built with.
     *
     * @return a java version string
     */
    public String getJavaVersion() {
        return javaVersion;
    }

    /**
     * Whether or not the plugin has a native controller.
     *
     * @return {@code true} if the plugin has a native controller
     */
    public boolean hasNativeController() {
        return hasNativeController;
    }

    /**
     * The target folder name for the plugin.
     *
     * @return the custom folder name for the plugin if the folder name is specified, else return the id with kebab-case.
     */
    public String getTargetFolderName() {
        return (this.customFolderName == null || this.customFolderName.isEmpty()) ? this.name : this.customFolderName;
    }

    /**
     * Returns actions requested by this plugin in the plugin-permissions.yml file
     *
     * @return A settings object containing the contents of plugin-permissions.yml file
     */
    public Settings getRequestedActions() {
        return requestedActions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("name", name);
            builder.field("version", version);
            builder.field("opensearch_version", getOpenSearchVersionRangesString());
            builder.field("java_version", javaVersion);
            builder.field("description", description);
            builder.field("classname", classname);
            builder.field("custom_foldername", customFolderName);
            builder.field("extended_plugins", extendedPlugins);
            builder.field("has_native_controller", hasNativeController);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginInfo that = (PluginInfo) o;

        if (!name.equals(that.name)) return false;
        // TODO: since the plugins are unique by their directory name, this should only be a name check, version should not matter?
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(String prefix) {
        final StringBuilder information = new StringBuilder().append(prefix)
            .append("- Plugin information:\n")
            .append(prefix)
            .append("Name: ")
            .append(name)
            .append("\n")
            .append(prefix)
            .append("Description: ")
            .append(description)
            .append("\n")
            .append(prefix)
            .append("Version: ")
            .append(version)
            .append("\n")
            .append(prefix)
            .append("OpenSearch Version: ")
            .append(getOpenSearchVersionRangesString())
            .append("\n")
            .append(prefix)
            .append("Java Version: ")
            .append(javaVersion)
            .append("\n")
            .append(prefix)
            .append("Native Controller: ")
            .append(hasNativeController)
            .append("\n")
            .append(prefix)
            .append("Extended Plugins: ")
            .append(extendedPlugins)
            .append("\n")
            .append(prefix)
            .append(" * Classname: ")
            .append(classname)
            .append("\n")
            .append(prefix)
            .append("Folder name: ")
            .append(customFolderName);
        return information.toString();
    }
}
