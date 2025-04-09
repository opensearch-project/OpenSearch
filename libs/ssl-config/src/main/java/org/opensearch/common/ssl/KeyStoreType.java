/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Enum representing the types of KeyStores supported by {@link KeyStoreFactory}.
 */
public enum KeyStoreType {

    JKS("JKS"),
    PKCS_12("PKCS12"),
    PKCS_11("PKCS11"),
    BCFKS("BCFKS");

    public static final Map<KeyStoreType, List<String>> TYPE_TO_EXTENSION_MAP = new HashMap<>();

    static {
        TYPE_TO_EXTENSION_MAP.put(JKS, List.of(".jks", ".ks"));
        TYPE_TO_EXTENSION_MAP.put(PKCS_12, List.of(".p12", ".pkcs12", ".pfx"));
        TYPE_TO_EXTENSION_MAP.put(BCFKS, List.of(".bcfks")); // Bouncy Castle FIPS Keystore
    }

    /**
     * Specifies KeyStore formats that are appropriate for use in a FIPS-compliant JVM:
     * - BCFKS KeyStore is specifically designed for FIPS compliance.
     * - PKCS#11 is vendor-specific and requires proper configuration to operate in FIPS mode.
     */
    public static final List<KeyStoreType> SECURE_KEYSTORE_TYPES = List.of(PKCS_11, BCFKS);

    private final String jcaName;

    KeyStoreType(String jks) {
        jcaName = jks;
    }

    public String getJcaName() {
        return jcaName;
    }

    public static KeyStoreType inferStoreType(String filePath) {
        return TYPE_TO_EXTENSION_MAP.entrySet()
            .stream()
            .filter(entry -> entry.getValue().stream().anyMatch(filePath::endsWith))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown keystore type for file path: " + filePath));
    }

    public static KeyStoreType getByJcaName(String value) {
        return Stream.of(KeyStoreType.values()).filter(type -> type.getJcaName().equals(value)).findFirst().orElse(null);
    }

}
