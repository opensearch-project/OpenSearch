/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchProviderException;
import java.util.stream.Collectors;

import static org.opensearch.common.crypto.KeyStoreType.SECURE_KEYSTORE_TYPES;
import static org.opensearch.common.crypto.KeyStoreType.inferStoreType;

/**
 * Restricts types of keystores to PKCS#11 and BCFKS when running in FIPS JVM.
 * Returns the keystore from specified provider or otherwise follows the priority of
 * declared security providers and their support for different keystores.
 */
public final class KeyStoreFactory {

    /**
     * Make a best guess about the "type" (see {@link KeyStore#getType()}) of the keystore file located at the given {@code Path}.
     * This method only references the <em>file name</em> of the keystore, it does not look at its contents.
     */
    public static KeyStore getInstanceBasedOnFileExtension(String filePath) {
        return getInstance(inferStoreType(filePath));
    }

    public static KeyStore getInstance(KeyStoreType type) {
        return getInstance(type, null);
    }

    public static KeyStore getInstance(KeyStoreType type, String provider) {
        if (CryptoServicesRegistrar.isInApprovedOnlyMode() && !SECURE_KEYSTORE_TYPES.contains(type)) {
            var secureKeyStoreNames = SECURE_KEYSTORE_TYPES.stream().map(KeyStoreType::name).collect(Collectors.joining(", "));
            throw new SecurityException("Only " + secureKeyStoreNames + " keystores are allowed in FIPS JVM");
        }

        try {
            if (provider == null) {
                return KeyStore.getInstance(type.getJcaName());
            }
            return KeyStore.getInstance(type.getJcaName(), provider);
        } catch (KeyStoreException | NoSuchProviderException e) {
            throw new SecurityException(e);
        }
    }

}
