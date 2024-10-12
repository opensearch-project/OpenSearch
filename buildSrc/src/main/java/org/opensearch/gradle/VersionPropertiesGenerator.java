/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

/**
 * Generator for shared dependency versions used by opensearch, namely the opensearch and lucene versions.
 */
public class VersionPropertiesGenerator {

    public static Properties generateVersionProperties(File tomlFile) throws Exception {
        TomlParseResult toml = Toml.parse(new FileReader(tomlFile));
        Properties properties = new Properties();

        toml.getTable("versions").keySet().forEach(key -> {
            String version = toml.getString("versions." + key);
            properties.setProperty(key, version);
        });
        loadBuildSrcVersion(properties, System.getProperties());
        return properties;
    }

    private static void loadBuildSrcVersion(Properties loadedProps, Properties systemProperties) {
        String opensearch = loadedProps.getProperty("opensearch");
        if (opensearch == null) {
            throw new IllegalStateException("OpenSearch version is missing from properties.");
        }
        if (opensearch.matches("[0-9]+\\.[0-9]+\\.[0-9]+") == false) {
            throw new IllegalStateException("Expected opensearch version to be numbers only of the form  X.Y.Z but it was: " + opensearch);
        }
        String qualifier = systemProperties.getProperty("build.version_qualifier", "");
        if (qualifier.isEmpty() == false) {
            if (qualifier.matches("(alpha|beta|rc)\\d+") == false) {
                throw new IllegalStateException("Invalid qualifier: " + qualifier);
            }
            opensearch += "-" + qualifier;
        }
        final String buildSnapshotSystemProperty = systemProperties.getProperty("build.snapshot", "true");
        switch (buildSnapshotSystemProperty) {
            case "true":
                opensearch += "-SNAPSHOT";
                break;
            case "false":
                // do nothing
                break;
            default:
                throw new IllegalArgumentException(
                    "build.snapshot was set to [" + buildSnapshotSystemProperty + "] but can only be unset or [true|false]"
                );
        }
        loadedProps.put("opensearch", opensearch);
    }
}
