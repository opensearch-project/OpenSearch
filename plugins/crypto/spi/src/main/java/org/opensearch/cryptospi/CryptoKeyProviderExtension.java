/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cryptospi;

import org.opensearch.common.settings.Settings;

/**
 * Extension of crypto to provide support for custom key providers.
 */
public interface CryptoKeyProviderExtension {

    /**
     * Every call to this method should return a new key provider.
     * @param settings These are crypto settings needed for creation of a new key provider.
     * @return master key provider.
     */
    MasterKeyProvider createKeyProvider(Settings settings);

    /**
     * One crypto plugin extension implementation refers to a unique key provider type.
     * @return key provider type
     */
    String type();
}
