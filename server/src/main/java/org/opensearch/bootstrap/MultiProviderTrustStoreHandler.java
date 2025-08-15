/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.fips.truststore.FipsTrustStore;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
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
import java.util.stream.Collectors;

import reactor.util.annotation.NonNull;

import static org.opensearch.cluster.fips.truststore.FipsTrustStore.SETTING_CLUSTER_FIPS_TRUSTSTORE_SOURCE;

/**
 * This class is responsible for handling and configuring trust stores from multiple providers
 * in FIPS-compliant environments. It enforces explicit trust store configuration to ensure
 * FIPS compliance and prevents accidental fallback to the non-compliant default JVM trust store.
 */
public class MultiProviderTrustStoreHandler {

    private static final Logger logger = LogManager.getLogger(MultiProviderTrustStoreHandler.class);
    private static final String TEMP_TRUSTSTORE_PREFIX = "converted-truststore-";
    private static final String TEMP_TRUSTSTORE_SUFFIX = ".bcfks";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PROVIDER = "javax.net.ssl.trustStoreProvider";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
    private static final String TRUST_STORE_PASSWORD = Security.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
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
    protected record TrustStoreProperties(String trustStorePath, String trustStoreType, String trustStorePassword,
        String trustStoreProvider) {
        void logout() {
            var detailLog = new StringWriter();
            var writer = new PrintWriter(detailLog);
            var passwordSetStatus = trustStorePassword.isEmpty() ? "[NOT SET]" : "[SET]";

            writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE + ": " + trustStorePath);
            writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE_TYPE + ": " + trustStoreType);
            writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE_PROVIDER + ": " + trustStoreProvider);
            writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE_PASSWORD + ": " + passwordSetStatus);
            writer.flush();

            logger.info("\nChanged TrustStore configuration: {}", detailLog);
        }
    }

    /**
     * <p>The method follows a strict 2-step resolution process:
     * <ol>
     *   <li><strong>JVM Parameters:</strong> Checks for explicitly configured trust stores
     *       via JVM system properties (e.g., {@code javax.net.ssl.trustStore})</li>
     *   <li><strong>Cluster Settings:</strong> Falls back to cluster-defined trust store settings
     *       if no JVM configuration is found</li>
     * </ol>
     *
     * <p>Supported trust store formats include:
     * <ul>
     *   <li>PKCS#11 (for NSS-backed hardware security)</li>
     *   <li>BCFKS (BouncyCastle FIPS KeyStore for software-based FIPS compliance)</li>
     * </ul>
     *
     * <p><strong>Important:</strong> If neither JVM parameters nor cluster settings define a trust store,
     * this method throws an exception. There is no automatic conversion or fallback to default
     * JDK trust stores, as this would compromise FIPS compliance.
     *
     * @param settings        used to look up cluster-defined trust store setting
     * @param tmpDir          the directory to store temporary TrustStore files
     * @param javaHome        the path to the Java Home, used for accessing default system TrustStore
     */
    public static void configureTrustStore(@NonNull Settings settings, @NonNull Path tmpDir, @NonNull Path javaHome) {
        var existingTrustStorePath = existingTrustStorePath();
        if (existingTrustStorePath != null) {
            logger.info("Custom truststore already specified: {}", existingTrustStorePath);
            return;
        }

        var fipsTrustStoreSetting = FipsTrustStore.TrustStoreSource.parse(settings.get(SETTING_CLUSTER_FIPS_TRUSTSTORE_SOURCE));
        var properties = switch (fipsTrustStoreSetting) {
            case PREDEFINED -> throw new IllegalArgumentException(
                "In FIPS JVM it is required for default TrustStore to be configured "
                    + "by '%s' JVM parameter or by '%s' cluster setting".formatted(
                        JAVAX_NET_SSL_TRUST_STORE,
                        SETTING_CLUSTER_FIPS_TRUSTSTORE_SOURCE
                    )
            );
            case SYSTEM_TRUST -> findPKCS11ProviderService().map(MultiProviderTrustStoreHandler::configurePKCS11TrustStore)
                .orElseThrow(() -> new IllegalStateException("No PKCS11 provider found, check java.security configuration for FIPS."));
            case GENERATED -> MultiProviderTrustStoreHandler.configureBCFKSTrustStore(javaHome, tmpDir);
        };

        setProperties(properties);
        properties.logout();
        printCurrentConfiguration(Level.TRACE);
    }

    @SuppressForbidden(reason = "check system properties for TrustStore configuration")
    private static Path existingTrustStorePath() {
        var property = Optional.ofNullable(System.getProperty(JAVAX_NET_SSL_TRUST_STORE));
        if (property.isPresent() && Path.of(property.get()).toFile().exists()) {
            return Path.of(property.get());
        }
        return null;
    }

    @SuppressForbidden(reason = "sets system properties for TrustStore configuration")
    protected static void setProperties(TrustStoreProperties properties) {
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE, properties.trustStorePath());
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, properties.trustStorePassword());
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, properties.trustStoreType());
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE_PROVIDER, properties.trustStoreProvider());
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
        var cacertsPath = javaHome.resolve("lib").resolve("security").resolve("cacerts");
        if (!Files.exists(cacertsPath) || !Files.isReadable(cacertsPath)) {
            throw new IllegalStateException("System cacerts not found at: " + cacertsPath);
        }

        logger.info("Loading system truststore from: {}", cacertsPath);

        KeyStore systemKs = null;
        for (var type : KNOWN_JDK_TRUSTSTORE_TYPES) {
            try {
                systemKs = KeyStore.getInstance(type);
                try (var is = readFileWithPriviledges(cacertsPath)) {
                    systemKs.load(is, JVM_DEFAULT_PASSWORD.toCharArray());
                }
                int certCount = systemKs.size();
                logger.info("Loaded {} certificates from system truststore", certCount);
                logger.info("Successfully loaded cacerts as {} format", type);
                break;
            } catch (Exception e) {
                logger.info("Failed to load cacerts as {}: {}", type, e.getMessage());
                systemKs = null;
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

    @SuppressWarnings("removal")
    private static InputStream readFileWithPriviledges(Path path) {
        return AccessController.doPrivileged((PrivilegedAction<InputStream>) () -> {
            try {
                return Files.newInputStream(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
                logger.warn("Failed to delete temporary file: {}", e.getMessage());
            }
        }));

        logger.info("Converting to BCFKS format: {}", tempBcfksFile.toAbsolutePath());

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
                            logger.warn("Failed to copy certificate '{}': {}", alias, e.getMessage());
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
            logger.info("Successfully converted {} certificates to BCFKS format", copiedCount);
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException("Failed to write BCFKS keystore", e);
        }

        return tempBcfksFile;
    }

    /**
     * Utility method to check the current configuration.
     */
    public static void printCurrentConfiguration(Level logLevel) {
        if (logger.isEnabled(logLevel)) {
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
            logger.log(logLevel, "\nAvailable Security Providers: \n{}", detailLog);
        }
    }
}
