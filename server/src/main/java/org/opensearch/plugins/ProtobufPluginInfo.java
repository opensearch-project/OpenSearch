/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.plugins;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.bootstrap.JarHell;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
public class ProtobufPluginInfo implements ProtobufWriteable {

    public static final String OPENSEARCH_PLUGIN_PROPERTIES = "plugin-descriptor.properties";
    public static final String OPENSEARCH_PLUGIN_POLICY = "plugin-security.policy";

    private final String name;
    private final String description;
    private final String version;
    private final Version opensearchVersion;
    private final String javaVersion;
    private final String classname;
    private final String customFolderName;
    private final List<String> extendedPlugins;
    private final boolean hasNativeController;

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
    public ProtobufPluginInfo(
        String name,
        String description,
        String version,
        Version opensearchVersion,
        String javaVersion,
        String classname,
        String customFolderName,
        List<String> extendedPlugins,
        boolean hasNativeController
    ) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.opensearchVersion = opensearchVersion;
        this.javaVersion = javaVersion;
        this.classname = classname;
        this.customFolderName = customFolderName;
        this.extendedPlugins = Collections.unmodifiableList(extendedPlugins);
        this.hasNativeController = hasNativeController;
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
    public ProtobufPluginInfo(
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
            hasNativeController
        );
    }

    /**
     * Construct plugin info from a stream.
    *
    * @param in the stream
    * @throws IOException if an I/O exception occurred reading the plugin info from the stream
    */
    public ProtobufPluginInfo(final CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput();
        this.name = in.readString();
        this.description = in.readString();
        this.version = in.readString();
        this.opensearchVersion = Version.readVersionProtobuf(in);
        this.javaVersion = in.readString();
        this.classname = in.readString();
        this.customFolderName = in.readString();
        this.extendedPlugins = protobufStreamInput.readList(CodedInputStream::readString, in);
        this.hasNativeController = in.readBool();
    }

    @Override
    public void writeTo(final CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput();
        out.writeStringNoTag(name);
        out.writeStringNoTag(description);
        out.writeStringNoTag(version);
        out.writeInt32NoTag(opensearchVersion.id);
        out.writeStringNoTag(javaVersion);
        out.writeStringNoTag(classname);
        if (customFolderName != null) {
            out.writeStringNoTag(customFolderName);
        } else {
            out.writeStringNoTag(name);
        }
        protobufStreamOutput.writeCollection(extendedPlugins, CodedOutputStream::writeStringNoTag, out);
        out.writeBoolNoTag(hasNativeController);
    }

    /**
     * Reads the plugin descriptor file.
    *
    * @param path           the path to the root directory for the plugin
    * @return the plugin info
    * @throws IOException if an I/O exception occurred reading the plugin descriptor
    */
    public static ProtobufPluginInfo readFromProperties(final Path path) throws IOException {
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
        if (opensearchVersionString == null) {
            throw new IllegalArgumentException("property [opensearch.version] is missing for plugin [" + name + "]");
        }
        final Version opensearchVersion = Version.fromString(opensearchVersionString);
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

        return new ProtobufPluginInfo(
            name,
            description,
            version,
            opensearchVersion,
            javaVersionString,
            classname,
            customFolderName,
            extendedPlugins,
            hasNativeController
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
     * The version of OpenSearch the plugin was built for.
    *
    * @return an OpenSearch version
    */
    public Version getOpenSearchVersion() {
        return opensearchVersion;
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
}
