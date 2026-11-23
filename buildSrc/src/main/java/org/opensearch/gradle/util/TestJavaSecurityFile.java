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
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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
 * <p>
 * The build-tools jar carries a copy of {@code fips_java.security} (see buildSrc/build.gradle), so plugins built outside
 * this repository get the same FIPS providers as the distribution without a copy of their own.
 */
public final class TestJavaSecurityFile {

    static final String PROVIDER_KEY_PREFIX = "security.provider.";
    static final String BC_FIPS_PROVIDER_ENTRY = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider C:HYBRID;ENABLE{All};";
    static final String BUNDLED_FIPS_RESOURCE = "fips_java.security";

    private TestJavaSecurityFile() {}

    /**
     * Returns the content of the {@code fips_java.security} that the build-tools jar carries.
     */
    public static String bundledFipsJavaSecurity() {
        return BundledFipsJavaSecurity.CONTENT;
    }

    /**
     * Writes the FIPS file for the JDK at {@code javaHome} to {@code target}. {@code fipsJavaSecurity} is the content of a
     * {@code fips_java.security} file, usually {@link #bundledFipsJavaSecurity()}.
     */
    public static void writeFips(Path javaHome, String fipsJavaSecurity, Path target) {
        try {
            final Properties fipsProperties = new Properties();
            fipsProperties.load(new StringReader(fipsJavaSecurity));
            write(
                forFips(loadJdkProperties(javaHome), fipsProperties),
                target,
                "the java.security of " + javaHome + " and the fips_java.security of the build tools"
            );
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

    /**
     * Reads the resource once, on first use, so that builds without FIPS mode never touch it.
     */
    private static final class BundledFipsJavaSecurity {
        static final String CONTENT = read();

        private static String read() {
            try (InputStream in = TestJavaSecurityFile.class.getClassLoader().getResourceAsStream(BUNDLED_FIPS_RESOURCE)) {
                if (in == null) {
                    throw new IllegalStateException("The build-tools jar does not contain [" + BUNDLED_FIPS_RESOURCE + "].");
                }
                // ISO-8859-1 is the encoding Properties#load(InputStream) assumes.
                return new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read [" + BUNDLED_FIPS_RESOURCE + "] from the build-tools jar.", e);
            }
        }
    }
}
