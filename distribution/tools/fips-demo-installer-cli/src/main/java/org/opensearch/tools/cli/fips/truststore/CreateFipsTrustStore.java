/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import picocli.CommandLine;

/**
 * Utility class for creating FIPS-compliant trust stores.
 * Converts JVM default trust stores to BCFKS format for FIPS compliance.
 */
public class CreateFipsTrustStore {

    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
    private static final String TRUST_STORE_PASSWORD = Security.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
    private static final String JVM_DEFAULT_PASSWORD = Objects.requireNonNullElse(TRUST_STORE_PASSWORD, "changeit");
    private static final String BCFKS = "BCFKS";
    private static final String BCFIPS = "BCFIPS";
    private static final List<String> KNOWN_JDK_TRUSTSTORE_TYPES = List.of("PKCS12", "JKS");

    public static KeyStore loadJvmDefaultTrustStore(CommandLine.Model.CommandSpec spec, Path javaHome) {
        var cacertsPath = javaHome.resolve("lib").resolve("security").resolve("cacerts");
        if (!Files.exists(cacertsPath) || !Files.isReadable(cacertsPath)) {
            throw new IllegalStateException("System cacerts not found at: " + cacertsPath);
        }

        spec.commandLine().getOut().println("Loading system truststore from: " + cacertsPath);

        KeyStore jvmKeyStore = null;
        for (var type : KNOWN_JDK_TRUSTSTORE_TYPES) {
            try {
                jvmKeyStore = KeyStore.getInstance(type);
                try (var is = readFileWithPrivilege(cacertsPath)) {
                    jvmKeyStore.load(is, JVM_DEFAULT_PASSWORD.toCharArray());
                }
                int certCount = jvmKeyStore.size();
                spec.commandLine().getOut().println("Loaded " + certCount + " certificates from system truststore");
                spec.commandLine().getOut().println("Successfully loaded cacerts as " + type + " format");
                break;
            } catch (Exception e) {
                jvmKeyStore = null;
                // continue
            }
        }

        if (jvmKeyStore == null) {
            throw new IllegalStateException(
                "Could not load system cacerts in any known format "
                    + KNOWN_JDK_TRUSTSTORE_TYPES.stream().collect(Collectors.joining(", ", "[", "]"))
            );
        }

        return jvmKeyStore;
    }

    public static ConfigurationProperties configureBCFKSTrustStore(Path bcfksPath, String password) {
        return new ConfigurationProperties(bcfksPath.toAbsolutePath().toString(), BCFKS, password, BCFIPS);
    }

    /**
     * Creates BCFKS formatted trustStore
     */
    public static Path convertToBCFKS(
        CommandLine.Model.CommandSpec spec,
        KeyStore sourceKeyStore,
        CommonOptions options,
        String password,
        Path confPath
    ) {
        Path trustStorePath = confPath.resolve("opensearch-fips-truststore.bcfks");

        if (Files.exists(trustStorePath)) {
            if (options.force) {
                try {
                    Files.delete(trustStorePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException("Operation cancelled. Trust store file already exists.");
            }
        }

        spec.commandLine().getOut().println("Converting to BCFKS format: " + trustStorePath.toAbsolutePath());

        int copiedCount = 0;
        KeyStore bcfksKeyStore;
        try {
            bcfksKeyStore = KeyStore.getInstance(BCFKS, BCFIPS);
            bcfksKeyStore.load(null, password.toCharArray());

            copyCerts(spec, sourceKeyStore, bcfksKeyStore, copiedCount);
            writeBCFKSKeyStoreToFile(bcfksKeyStore, trustStorePath, password);

            spec.commandLine()
                .getOut()
                .printf(
                    Locale.ROOT,
                    "Successfully converted %s/%s certificates to BCFKS format.%n",
                    bcfksKeyStore.size(),
                    sourceKeyStore.size()
                );

            if (sourceKeyStore.size() > bcfksKeyStore.size()) {
                spec.commandLine()
                    .getOut()
                    .printf(
                        Locale.ROOT,
                        "%s certificates could not be converted to BCFKS format.%n",
                        sourceKeyStore.size() - bcfksKeyStore.size()
                    );
            }
        } catch (GeneralSecurityException | IOException e) {
            throw new SecurityException(e);
        }

        return trustStorePath;
    }

    private static void copyCerts(CommandLine.Model.CommandSpec spec, KeyStore source, KeyStore target, int copiedCount)
        throws KeyStoreException {
        var aliases = source.aliases();

        while (aliases.hasMoreElements()) {
            var alias = aliases.nextElement();

            if (source.isCertificateEntry(alias)) {
                var cert = source.getCertificate(alias);
                if (cert != null) {
                    try {
                        target.setCertificateEntry(alias, cert);
                        copiedCount++;
                    } catch (Exception e) {
                        spec.commandLine().getOut().printf(Locale.ROOT, "Failed to copy certificate '%s': %s%n", alias, e.getMessage());
                        // Continue with other certificates
                    }
                }
            }
        }
    }

    private static void writeBCFKSKeyStoreToFile(KeyStore bcfksKeyStore, Path tempBcfksFile, String password) {
        try (var outputStream = Files.newOutputStream(tempBcfksFile)) {
            bcfksKeyStore.store(outputStream, password.toCharArray());
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException("Failed to write BCFKS keystore to [%s]: ".formatted(tempBcfksFile) + e.getMessage(), e);
        }
    }

    @SuppressWarnings("removal")
    private static InputStream readFileWithPrivilege(Path path) {
        return AccessController.doPrivileged((PrivilegedAction<InputStream>) () -> {
            try {
                return Files.newInputStream(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
