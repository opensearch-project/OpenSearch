/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.SuppressForbidden;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Permissions;
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class is responsible for handling and configuring trust stores from multiple providers.
 * It facilitates the interception of trust store configurations, conversion of default
 * system trust stores into desired formats, and provides utilities to manage the trust store
 * configuration in runtime environments.
 * The class supports handling PKCS#11 and BCFKS trust store types. If a custom trust store is
 * not specified, it intercepts the default "cacerts" trust store and converts it to an alternative
 * format if applicable. It uses security providers to dynamically determine and configure appropriate
 * KeyStore services.
 */
public class MultiProviderTrustStoreHandler {

    private static final Logger logger = Logger.getLogger(MultiProviderTrustStoreHandler.class.getName());
    private static final String TEMP_TRUSTSTORE_PREFIX = "converted-truststore-";
    private static final String TEMP_TRUSTSTORE_SUFFIX = ".bcfks";
    private static final String TRUST_STORE_PASSWORD = Security.getProperty("javax.net.ssl.trustStorePassword");
    private static final String JVM_DEFAULT_PASSWORD = Objects.requireNonNullElse(TRUST_STORE_PASSWORD, "changeit");
    private static final String SYSTEMSTORE_PASSWORD = Objects.requireNonNullElse(TRUST_STORE_PASSWORD, "");
    private static final String PKCS_11 = "PKCS11";
    private static final String BCFKS = "BCFKS";
    private static final String BCFIPS = "BCFIPS";
    private static final List<String> KNOWN_JDK_TRUSTSTORE_TYPES = List.of("PKCS12", "JKS");

    /**
     * Encapsulates properties related to a TrustStore configuration. Is primarily used to manage and
     * log details of TrustStore configurations within the system.
     */
    private record TrustStoreProperties(String trustStorePath, String trustStoreType, String trustStorePassword,
        String trustStoreProvider) {
        void logout() {
            var detailLog = new StringWriter();
            var writer = new PrintWriter(detailLog);
            var passwordSetStatus = trustStorePassword.isEmpty() ? "[NOT SET]" : "[SET]";

            writer.printf(Locale.ROOT, "\njavax.net.ssl.trustStore: " + trustStorePath);
            writer.printf(Locale.ROOT, "\njavax.net.ssl.trustStoreType: " + trustStoreType);
            writer.printf(Locale.ROOT, "\njavax.net.ssl.trustStoreProvider: " + trustStoreProvider);
            writer.printf(Locale.ROOT, "\njavax.net.ssl.trustStorePassword: " + passwordSetStatus);
            writer.flush();

            logger.info("\nChanged TrustStore configuration:" + detailLog);
        }
    }

    /**
     * Configures the Java TrustStore by setting up appropriate truststore properties.
     * If a custom TrustStore path is provided, it will use it directly;
     * otherwise, it will dynamically configure a TrustStore using a suitable provider.
     *
     * @param tmpDir          the directory to store temporary TrustStore files
     * @param javaHome        the path to the Java Home, used for accessing default system TrustStore
     */
    public static void configureTrustStore(Path tmpDir, Path javaHome) {
        var existingTrustStorePath = existingTrustStorePath();
        if (existingTrustStorePath != null) {
            logger.info("Custom truststore already specified: " + existingTrustStorePath);
            return;
        }

        logger.info("No custom truststore specified - intercepting default cacerts usage");
        var pkcs11ProviderService = findPKCS11ProviderService();
        var properties = pkcs11ProviderService.map(MultiProviderTrustStoreHandler::configurePKCS11TrustStore)
            .orElseGet(() -> MultiProviderTrustStoreHandler.configureBCFKSTrustStore(javaHome, tmpDir));

        setProperties(properties);
        logger.info("Converted system truststore to %s format".formatted(properties.trustStoreProvider));
        properties.logout();
        printCurrentConfiguration(Level.FINEST);
    }

    @SuppressForbidden(reason = "check system properties for TrustStore configuration")
    private static Path existingTrustStorePath() {
        var property = Optional.ofNullable(System.getProperty("javax.net.ssl.trustStore"));
        if (property.isPresent() && Path.of(property.get()).toFile().exists()) {
            return Path.of(property.get());
        }
        return null;
    }

    @SuppressForbidden(reason = "sets system properties for TrustStore configuration")
    protected static void setProperties(TrustStoreProperties properties) {
        System.setProperty("javax.net.ssl.trustStore", properties.trustStorePath());
        System.setProperty("javax.net.ssl.trustStorePassword", properties.trustStorePassword());
        System.setProperty("javax.net.ssl.trustStoreType", properties.trustStoreType());
        System.setProperty("javax.net.ssl.trustStoreProvider", properties.trustStoreProvider());
    }

    private static TrustStoreProperties configurePKCS11TrustStore(Provider.Service pkcs11ProviderService) {
        logger.info("Configuring PKCS11 truststore...");
        return new TrustStoreProperties("NONE", PKCS_11, SYSTEMSTORE_PASSWORD, pkcs11ProviderService.getProvider().getName());
    }

    private static TrustStoreProperties configureBCFKSTrustStore(Path javaHome, Path tmpDir) {
        logger.info("No PKCS11 provider found - converting system truststore to BCFKS...");
        var systemTrustStore = loadSystemDefaultTrustStore(javaHome);
        var bcfksTrustStore = convertToBCFKS(systemTrustStore, tmpDir);
        return new TrustStoreProperties(bcfksTrustStore.toAbsolutePath().toString(), BCFKS, JVM_DEFAULT_PASSWORD, BCFIPS);
    }

    public static Optional<Provider.Service> findPKCS11ProviderService() {
        return Arrays.stream(Security.getProviders())
            .filter(it -> it.getName().toUpperCase(Locale.ROOT).contains(PKCS_11))
            .map(it -> it.getService("KeyStore", PKCS_11))
            .filter(Objects::nonNull)
            .findFirst();
    }

    private static KeyStore loadSystemDefaultTrustStore(Path javaHome) {
        try {
            FilePermissionUtils.addSingleFilePath(new Permissions(), javaHome, "read,readlink");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var cacertsPath = javaHome.resolve("lib").resolve("security").resolve("cacerts");
        if (!Files.exists(cacertsPath) && Files.isReadable(cacertsPath)) {
            throw new IllegalStateException("System cacerts not found at: " + cacertsPath);
        }

        logger.info("Loading system truststore from: " + cacertsPath);

        KeyStore systemKs = null;
        for (var type : KNOWN_JDK_TRUSTSTORE_TYPES) {
            try {
                systemKs = KeyStore.getInstance(type);
                FilePermissionUtils.addSingleFilePath(new Permissions(), javaHome, "read,readlink");
                try (var is = Files.newInputStream(cacertsPath)) {
                    systemKs.load(is, JVM_DEFAULT_PASSWORD.toCharArray());
                }
                int certCount = systemKs.size();
                logger.info("Loaded " + certCount + " certificates from system truststore");
                logger.info("Successfully loaded cacerts as " + type + " format");
                break;
            } catch (Exception e) {
                logger.info("Failed to load cacerts as " + type + ": " + e.getMessage());
                // continue
            }
        }

        if (systemKs == null) {
            throw new IllegalStateException(
                "Could not load system cacerts in any known format"
                    + KNOWN_JDK_TRUSTSTORE_TYPES.stream().collect(Collectors.joining(", ", "[", "]"))
            );
        }

        return systemKs;
    }

    /**
     * Creates a temporary BCFKS formatted trustStore
     */
    private static Path convertToBCFKS(KeyStore sourceKeyStore, Path tmpDir) {
        Path tempBcfksFile;
        try {
            tempBcfksFile = Files.createTempFile(tmpDir, TEMP_TRUSTSTORE_PREFIX, TEMP_TRUSTSTORE_SUFFIX);
        } catch (IOException ex) {
            throw new IllegalStateException("Could not create temporary truststore file", ex);
        }

        // Register for deletion on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.deleteIfExists(tempBcfksFile);
            } catch (IOException e) {
                logger.warning("Failed to delete temporary file: " + e.getMessage());
            }
        }));

        logger.info("Converting to BCFKS format: " + tempBcfksFile.toAbsolutePath());

        // Create new BCFKS keystore
        int copiedCount = 0;
        KeyStore bcfksKeyStore;
        try {
            bcfksKeyStore = KeyStore.getInstance(BCFKS, BCFIPS);
            bcfksKeyStore.load(null, null);

            // Copy all certificates from source to BCFKS
            Enumeration<String> aliases = sourceKeyStore.aliases();

            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();

                if (sourceKeyStore.isCertificateEntry(alias)) {
                    Certificate cert = sourceKeyStore.getCertificate(alias);
                    if (cert != null) {
                        try {
                            bcfksKeyStore.setCertificateEntry(alias, cert);
                            copiedCount++;
                        } catch (Exception e) {
                            logger.warning("Failed to copy certificate '" + alias + "': " + e.getMessage());
                            // Continue with other certificates
                        }
                    }
                }
            }
        } catch (GeneralSecurityException | IOException e) {
            throw new SecurityException(e);
        }

        // Save BCFKS keystore to file using NIO.2
        try (var file = Files.newOutputStream(tempBcfksFile)) {
            bcfksKeyStore.store(file, JVM_DEFAULT_PASSWORD.toCharArray());
            logger.info("Successfully converted " + copiedCount + " certificates to BCFKS format");
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException("Failed to write BCFKS keystore", e);
        }

        return tempBcfksFile;
    }

    /**
     * Utility method to check the current configuration.
     */
    public static void printCurrentConfiguration(Level logLevel) {
        if (logger.isLoggable(logLevel)) {
            var detailLog = new StringWriter();
            var writer = new PrintWriter(detailLog);
            var counter = new AtomicInteger();
            Arrays.stream(Security.getProviders())
                .peek(
                    provider -> writer.printf(
                        Locale.ROOT,
                        "  %d. %s (version %s)\n".formatted(counter.incrementAndGet(), provider.getName(), provider.getVersionStr())
                    )
                )
                .flatMap(provider -> provider.getServices().stream().filter(service -> "KeyStore".equals(service.getType())))
                .forEach(service -> writer.printf(Locale.ROOT, "\t\tKeyStore.%s\n".formatted(service.getAlgorithm())));

            writer.flush();
            logger.log(logLevel, "\nAvailable Security Providers:\n" + detailLog);
        }
    }
}
