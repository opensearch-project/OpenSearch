/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

/**
 * Validator for FIPS-compliant SSL trust store configuration.
 *
 * <p>This utility validates that the required SSL trust store properties are properly configured
 * for FIPS mode operation. It ensures that all necessary system properties are set and that
 * the trust store file is accessible and valid.</p>
 */
public class FipsTrustStoreValidator {

    private static final Logger LOGGER = LogManager.getLogger(FipsTrustStoreValidator.class);
    private static final String TRUST_STORE_PATH_PROP = "javax.net.ssl.trustStore";
    private static final String TRUST_STORE_TYPE_PROP = "javax.net.ssl.trustStoreType";
    private static final String TRUST_STORE_PROVIDER_PROP = "javax.net.ssl.trustStoreProvider";
    private static final String TRUST_STORE_PASSWORD_PROP = "javax.net.ssl.trustStorePassword";
    private static final String ALLOWED_TYPE_PKCS11 = "PKCS11";
    private static final String ALLOWED_TYPE_BCFKS = "BCFKS";

    /**
     * Validates the FIPS trust store configuration from system properties.
     *
     * @throws IllegalStateException if the trust store configuration is invalid
     */
    public static void validate() {
        validate(
            System.getProperty(TRUST_STORE_PATH_PROP, ""),
            System.getProperty(TRUST_STORE_TYPE_PROP, ""),
            System.getProperty(TRUST_STORE_PROVIDER_PROP, ""),
            System.getProperty(TRUST_STORE_PASSWORD_PROP, "")
        );
    }

    /**
     * Validates the FIPS trust store configuration with explicit parameters.
     *
     * @param trustStorePath the path to the trust store file
     * @param trustStoreType the type of the trust store (must be PKCS11 or BCFKS for FIPS compliance)
     * @param trustStoreProvider the security provider name
     * @param trustStorePassword the trust store password
     * @throws IllegalStateException if the trust store configuration is invalid
     */
    protected static void validate(String trustStorePath, String trustStoreType, String trustStoreProvider, String trustStorePassword) {
        if (trustStoreType.isBlank()) {
            throw new IllegalStateException(
                "Trust store type must be specified using the '-Djavax.net.ssl.trustStoreType' JVM option. Accepted values are PKCS11 and BCFKS."
            );
        }

        var requiredProperties = switch (trustStoreType) {
            case ALLOWED_TYPE_PKCS11 -> Map.ofEntries(
                entry(TRUST_STORE_TYPE_PROP, trustStoreType),
                entry(TRUST_STORE_PROVIDER_PROP, trustStoreProvider)
            );
            case ALLOWED_TYPE_BCFKS -> Map.ofEntries(
                entry(TRUST_STORE_PATH_PROP, trustStorePath),
                entry(TRUST_STORE_TYPE_PROP, trustStoreType),
                entry(TRUST_STORE_PROVIDER_PROP, trustStoreProvider)
            );
            default -> throw new IllegalStateException(
                "Trust store type must be PKCS11 or BCFKS for FIPS compliance. Found: " + trustStoreType
            );
        };

        var missingProperties = requiredProperties.entrySet().stream().filter(entry -> entry.getValue().isBlank()).toList();
        if (!missingProperties.isEmpty()) {
            throw new IllegalStateException(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\n"
                    + missingProperties.stream().map(Map.Entry::getKey).collect(Collectors.joining("\n"))
            );
        }

        if (ALLOWED_TYPE_BCFKS.equals(trustStoreType)) {
            validateBCFKSFile(trustStorePath);
        }

        try {
            var provider = Security.getProvider(trustStoreProvider);
            if (provider == null) {
                throw new IllegalStateException("Trust store provider not available: " + trustStoreProvider);
            }

            var keyStore = KeyStore.getInstance(trustStoreType, provider);

            switch (trustStoreType) {
                case ALLOWED_TYPE_PKCS11 -> keyStore.load(null, trustStorePassword.toCharArray());
                case ALLOWED_TYPE_BCFKS -> {
                    try (var inputStream = Files.newInputStream(Path.of(trustStorePath))) {
                        keyStore.load(inputStream, trustStorePassword.toCharArray());
                    }
                }
            }

            if (keyStore.size() == 0) {
                LOGGER.warn("Trust store is valid but contains no certificates (type: {})", trustStoreType);
            }

            LOGGER.debug(
                "Trust store validation successful (type: {}, provider: {}, certificates: {})",
                trustStoreType,
                provider.getName(),
                keyStore.size()
            );
        } catch (KeyStoreException e) {
            throw new IllegalStateException("Invalid trust store type or provider: " + e.getMessage(), e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Trust store algorithm not supported: " + e.getMessage(), e);
        } catch (CertificateException e) {
            throw new IllegalStateException("Trust store contains invalid certificates: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read trust store file (possibly wrong password): " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException("Trust store validation failed: " + e.getMessage(), e);
        }
    }

    private static void validateBCFKSFile(String trustStorePath) {
        var trustStoreFile = Path.of(trustStorePath);
        if (!Files.exists(trustStoreFile)) {
            throw new IllegalStateException("Trust store file does not exist: " + trustStorePath);
        }

        if (!Files.isReadable(trustStoreFile)) {
            throw new IllegalStateException("Trust store file is not readable: " + trustStorePath);
        }

        try {
            if (Files.size(trustStoreFile) == 0) {
                throw new IllegalStateException("Trust store file is empty: " + trustStorePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot access trust store file: " + e.getMessage(), e);
        }
    }

}
