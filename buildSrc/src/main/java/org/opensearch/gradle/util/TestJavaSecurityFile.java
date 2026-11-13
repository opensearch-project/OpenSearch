/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Builds the {@code java.security} file that test JVMs load as a complete replacement of the JDK's own file, the same way
 * {@code org.opensearch.tools.launchers.JavaSecurityFile} does for a distribution:
 * <ul>
 *     <li>FIPS: the JDK's properties without its providers, overridden by every key of
 *     {@code distribution/src/config/fips_java.security}.</li>
 *     <li>Default: the JDK's properties, with the BouncyCastle FIPS provider declared after the JDK's providers. The JDK skips
 *     a declared provider whose class is not on the classpath.</li>
 * </ul>
 * Keep the two in sync; buildSrc cannot depend on the launchers project.
 */
public final class TestJavaSecurityFile {

    static final String PROVIDER_KEY_PREFIX = "security.provider.";
    static final String BC_FIPS_PROVIDER_ENTRY = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider C:HYBRID;ENABLE{All};";

    private TestJavaSecurityFile() {}

    /**
     * Writes the FIPS file for the JDK at {@code javaHome} to {@code target}.
     */
    public static void writeFips(Path javaHome, Path fipsFile, Path target) {
        try {
            write(forFips(loadJdkProperties(javaHome), load(fipsFile)), target, "the java.security of " + javaHome + " and " + fipsFile);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot generate the FIPS java.security file [" + target + "]", e);
        }
    }

    /**
     * Writes the default (non-FIPS) file for the JDK at {@code javaHome} to {@code target}.
     */
    public static void writeDefault(Path javaHome, Path target) {
        try {
            write(forDefault(loadJdkProperties(javaHome)), target, "the java.security of " + javaHome);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot generate the java.security file [" + target + "]", e);
        }
    }

    static Properties forFips(Properties jdkProperties, Properties fipsProperties) {
        final Properties result = new Properties();
        for (String key : jdkProperties.stringPropertyNames()) {
            final boolean isProvider = key.startsWith(PROVIDER_KEY_PREFIX);
            if (isProvider == false) {
                result.setProperty(key, jdkProperties.getProperty(key));
            }
        }
        for (String key : fipsProperties.stringPropertyNames()) {
            result.setProperty(key, fipsProperties.getProperty(key));
        }
        if (providerCount(result) == 0) {
            throw new IllegalStateException("the FIPS java.security file must declare at least [" + PROVIDER_KEY_PREFIX + "1]");
        }
        return result;
    }

    static Properties forDefault(Properties jdkProperties) {
        final Properties result = new Properties();
        for (String key : jdkProperties.stringPropertyNames()) {
            result.setProperty(key, jdkProperties.getProperty(key));
        }
        final int nextFreeSlot = providerCount(result) + 1;
        result.setProperty(PROVIDER_KEY_PREFIX + nextFreeSlot, BC_FIPS_PROVIDER_ENTRY);
        return result;
    }

    /**
     * Counts the providers the JDK will load: it stops at the first missing slot.
     */
    static int providerCount(Properties properties) {
        int count = 0;
        while (true) {
            final String nextProviderEntry = properties.getProperty(PROVIDER_KEY_PREFIX + (count + 1));
            if (nextProviderEntry == null) {
                break;
            }
            count++;
        }
        return count;
    }

    private static void write(Properties properties, Path target, String sources) throws IOException {
        Files.createDirectories(target.getParent());
        try (OutputStream out = Files.newOutputStream(target)) {
            properties.store(out, "Generated from " + sources);
        }
    }

    private static Properties loadJdkProperties(Path javaHome) throws IOException {
        return load(javaHome.resolve("conf/security/java.security"));
    }

    private static Properties load(Path file) throws IOException {
        final Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            properties.load(in);
        }
        return properties;
    }
}
