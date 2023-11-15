/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Crypto plugin to provide support for custom key providers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CryptoKeyProviderPlugin {

    /**
     * Every call to this method should return a new key provider.
     * @param cryptoMetadata These are crypto settings needed for creation of a new key provider.
     * @return master key provider.
     */
    MasterKeyProvider createKeyProvider(CryptoMetadata cryptoMetadata);

    /**
     * One crypto plugin extension implementation refers to a unique key provider type.
     * @return key provider type
     */
    String type();
}
