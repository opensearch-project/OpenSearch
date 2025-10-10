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
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.util.List;

/**
 * Validator for FIPS-compliant SSL trust store configuration.
 *
 * <p>This utility validates that the required SSL trust store properties are properly configured
 * for FIPS mode operation. It ensures that all necessary system properties are set and that
 * the trust store file is accessible and valid.</p>
 */
public class FipsTrustStoreValidator {

    private static final Logger logger = LogManager.getLogger(FipsTrustStoreValidator.class);

    public static void validateRequiredProperties() {
        var requiredProperties = List.of(
            "javax.net.ssl.trustStore",
            "javax.net.ssl.trustStoreType",
            "javax.net.ssl.trustStoreProvider",
            "javax.net.ssl.trustStorePassword"
        );

        var missingProperties = requiredProperties.stream().filter(prop -> {
            var value = System.getProperty(prop);
            return value == null || value.isBlank();
        }).toList();

        if (!missingProperties.isEmpty()) {
            throw new IllegalStateException(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\n" + String.join("\n", missingProperties)
            );
        }

        validateTrustStoreFile();
    }

    @SuppressForbidden(reason = "get path not configured in environment")
    private static void validateTrustStoreFile() {
        String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
        String trustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
        String trustStoreProvider = System.getProperty("javax.net.ssl.trustStoreProvider");
        String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");

        // 1. Check file existence and readability
        Path trustStoreFile = PathUtils.get(System.getProperty("javax.net.ssl.trustStore"));
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
            throw new IllegalStateException("Cannot access trust store file: " + e.getMessage(), e);
        }

        // 2. Verify JRE can load the trust store
        try {
            Provider provider = java.security.Security.getProvider(trustStoreProvider);
            if (provider == null) {
                throw new IllegalStateException("Trust store provider not available: " + trustStoreProvider);
            }

            // Create KeyStore instance
            KeyStore keyStore = KeyStore.getInstance(trustStoreType, provider);

            // Load the trust store to verify it's valid
            try (var inputStream = Files.newInputStream(trustStoreFile)) {
                keyStore.load(inputStream, trustStorePassword.toCharArray());
            }

            // Verify it contains certificates (optional check)
            if (keyStore.size() == 0) {
                logger.warn("Trust store is valid but contains no certificates: {}", trustStorePath);
            }

            logger.warn(
                "Trust store validation successful for {} (type: {}, provider: {}, certificates: {})",
                trustStorePath,
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
            throw new IllegalStateException("Cannot read trust store file (possibly wrong password): " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException("Trust store validation failed: " + e.getMessage(), e);
        }
    }

}
