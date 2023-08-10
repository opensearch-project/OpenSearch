/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.crypto.CryptoManager;

import java.util.Set;

/**
 * An extension point for {@link Plugin} implementations to provide client side encryption/decryption support.
 *
 * @opensearch.api
 */
public interface CryptoPlugin {

    /**
     * Crypto plugin name
     * @return crypto plugin name
     */
    String name();

    /**
     * @return Collection of registered key provider implementations
     */
    Set<String> getKeyProviderTypes();

    /**
     * Creates factory for the creation of crypto client.
     * @param keyProviderType Key provider type/implementation for which factory needs to be created.
     * @return Crypto client factory.
     */
    CryptoManager.Factory createClientFactory(String keyProviderType);
}
