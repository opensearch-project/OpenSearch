/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.fips.truststore;

import org.opensearch.common.settings.Setting;

import java.util.Locale;

/**
 * The class provides configuration and utilities for
 * managing trust store settings specifically for JVMs operating in FIPS mode.
 */
public class FipsTrustStore {

    /**
     * This enumeration provides distinct options for configuring the trust store behavior:
     * <ul>
     *   <li>SYSTEM_TRUST: Uses the system’s PKCS#11 provider for the trust store.</li>
     *   <li>GENERATED: Dynamically generates a BCFKS (BouncyCastle FIPS KeyStore) instance.</li>
     * </ul>
     */
    public enum TrustStoreSource {
        SYSTEM_TRUST,
        GENERATED;

        public static TrustStoreSource parse(String strValue) {
            if (strValue == null) {
                return null;
            } else {
                strValue = strValue.toUpperCase(Locale.ROOT);
                try {
                    return TrustStoreSource.valueOf(strValue);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Illegal truststore.source value [" + strValue + "]");
                }
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Defines the TrustStore source to use as default in FIPS JVM
     */
    public static final String SETTING_CLUSTER_FIPS_TRUSTSTORE_SOURCE = "cluster.fips.truststore.source";
    public static final Setting<TrustStoreSource> CLUSTER_FIPS_TRUSTSTORE_SOURCE_SETTING = new Setting<>(
        SETTING_CLUSTER_FIPS_TRUSTSTORE_SOURCE,
        "",
        TrustStoreSource::parse,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
}
