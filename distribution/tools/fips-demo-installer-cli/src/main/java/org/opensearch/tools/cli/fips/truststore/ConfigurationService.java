/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE;
import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_PROVIDER;
import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_TYPE;

/**
 * Service for managing OpenSearch configuration files.
 * Handles verification and writing of JVM options and security configurations.
 */
public class ConfigurationService {
    
    private static final String OPENSEARCH_CONF_PATH = System.getProperty("opensearch.path.conf");
    private static final String JVM_OPTIONS_FILE = OPENSEARCH_CONF_PATH + File.separatorChar + "jvm.options";

    public static void verifyJvmOptionsFile(CommonOptions options) {
        if (options.force) {
            System.out.println("WARNING: Force mode enabled, skipping configuration checks.");
            return;
        }

        var jvmOptionsFile = new File(JVM_OPTIONS_FILE);

        if (!jvmOptionsFile.exists()) {
            throw new IllegalStateException("jvm.options file does not exist: " + JVM_OPTIONS_FILE);
        }

        if (!jvmOptionsFile.canRead()) {
            throw new IllegalStateException("jvm.options file is not readable: " + JVM_OPTIONS_FILE);
        }

        if (jvmOptionsFile.length() == 0) {
            throw new IllegalStateException("jvm.options file is empty: " + JVM_OPTIONS_FILE);
        }

        try {
            String content = Files.readString(jvmOptionsFile.toPath(), StandardCharsets.UTF_8);

            String[] fipsProperties = {
                "-D" + JAVAX_NET_SSL_TRUST_STORE,
                "-D" + JAVAX_NET_SSL_TRUST_STORE_PASSWORD,
                "-D" + JAVAX_NET_SSL_TRUST_STORE_TYPE,
                "-D" + JAVAX_NET_SSL_TRUST_STORE_PROVIDER, };

            for (String property : fipsProperties) {
                if (content.contains(property)) {
                    throw new IllegalStateException(
                        "FIPS demo configuration already exists in jvm.options. "
                            + "Found: '"
                            + property.substring(0, property.length() - 1)
                            + "'. "
                            + "Please remove existing configuration before running this installer."
                    );
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read jvm.options file: " + e.getMessage(), e);
        }
    }

    public void writeSecurityConfigToJvmOptionsFile(ConfigurationProperties properties) {
        var configHeader = System.lineSeparator()
            + "################################################################"
            + System.lineSeparator()
            + "## Start OpenSearch FIPS Demo Configuration"
            + System.lineSeparator()
            + "## WARNING: revise all the lines below before you go into production"
            + System.lineSeparator()
            + "################################################################"
            + System.lineSeparator()
            + System.lineSeparator();
        var configFooter = System.lineSeparator()
            + "################################################################"
            + System.lineSeparator();

        try (FileWriter writer = new FileWriter(JVM_OPTIONS_FILE, StandardCharsets.UTF_8, true)) {
            writer.write(configHeader);

            // Write each configuration as -Dkey=value format
            var configMap = Map.of(
                JAVAX_NET_SSL_TRUST_STORE,
                properties.trustStorePath(),
                JAVAX_NET_SSL_TRUST_STORE_PASSWORD,
                properties.trustStorePassword(),
                JAVAX_NET_SSL_TRUST_STORE_TYPE,
                properties.trustStoreType(),
                JAVAX_NET_SSL_TRUST_STORE_PROVIDER,
                properties.trustStoreProvider()
            );

            for (Map.Entry<String, String> entry : configMap.entrySet()) {
                writer.write("-D" + entry.getKey() + "=" + entry.getValue());
                writer.write(System.lineSeparator());
            }

            writer.write(configFooter);
        } catch (IOException e) {
            throw new RuntimeException("Exception writing security configuration to jvm.options: " + e.getMessage(), e);
        }
    }
}
