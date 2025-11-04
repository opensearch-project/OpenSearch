/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import picocli.CommandLine;

import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE;
import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_PROVIDER;
import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_TYPE;

/**
 * Service for managing OpenSearch configuration files.
 * Handles verification and writing of JVM options and security configurations.
 */
public class ConfigurationService {

    /**
     * Verifies that the jvm.options file exists, is readable, and doesn't contain existing FIPS configuration.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param confPath path to the OpenSearch configuration directory
     * @throws IllegalStateException if validation fails
     */
    public static void verifyJvmOptionsFile(CommandLine.Model.CommandSpec spec, CommonOptions options, Path confPath) {
        var jvmOptionsFile = confPath.resolve("jvm.options");
        if (options.force) {
            var ansi = spec.commandLine().getColorScheme().ansi();
            spec.commandLine().getOut().println(ansi.string("@|yellow WARNING: Force mode enabled, skipping configuration checks.|@"));
            return;
        }

        if (!Files.exists(jvmOptionsFile)) {
            throw new IllegalStateException("jvm.options file does not exist: " + jvmOptionsFile);
        }

        if (!Files.isReadable(jvmOptionsFile)) {
            throw new IllegalStateException("jvm.options file is not readable: " + jvmOptionsFile);
        }

        validateJvmOptionsContent(jvmOptionsFile);
    }

    /**
     * Validates that the jvm.options file doesn't already contain FIPS trust store properties.
     *
     * @param jvmOptionsFile path to the jvm.options file
     * @throws IllegalStateException if FIPS configuration already exists
     */
    protected static void validateJvmOptionsContent(Path jvmOptionsFile) {
        try {
            var size = Files.size(jvmOptionsFile);
            if (size == 0) {
                throw new IllegalStateException("jvm.options file is empty: " + jvmOptionsFile);
            }

            var content = Files.readString(jvmOptionsFile, StandardCharsets.UTF_8);

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
                            + property
                            + "'. "
                            + "Please remove existing configuration before running this installer, or use the '--force option'"
                    );
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read jvm.options file: " + e.getMessage(), e);
        }
    }

    /**
     * Writes FIPS trust store configuration to the jvm.options file.
     *
     * @param properties the trust store configuration properties to write
     * @param confPath path to the OpenSearch configuration directory
     * @throws RuntimeException if writing fails
     */
    public void writeSecurityConfigToJvmOptionsFile(ConfigurationProperties properties, Path confPath) {
        var jvmOptionsFile = confPath.resolve("jvm.options");
        var configHeader = """

            ################################################################
            ## Start OpenSearch FIPS Demo Configuration
            ## WARNING: revise all the lines below before you go into production
            ################################################################

            """;
        var configFooter = """
            ################################################################
            """;

        try {
            var configBuilder = new StringBuilder(configHeader);

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
                configBuilder.append("-D").append(entry.getKey()).append("=").append(entry.getValue());
                configBuilder.append(System.lineSeparator());
            }

            configBuilder.append(configFooter);

            Files.writeString(
                jvmOptionsFile,
                configBuilder.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
            );
        } catch (IOException e) {
            throw new RuntimeException("Exception writing security configuration to jvm.options: " + e.getMessage(), e);
        }
    }
}
