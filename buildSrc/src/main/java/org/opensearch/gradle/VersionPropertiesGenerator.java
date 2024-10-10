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

        return properties;
    }
}
