/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.util.concurrent.RefCounted;

/**
 * Crypto plugin interface used for encryption and decryption.
 */
public interface CryptoManager extends RefCounted {

    /**
     * @return key provider type
     */
    String type();

    /**
     * @return key provider name
     */
    String name();

    /**
     * @return Crypto provider for encrypting or decrypting raw content.
     */
    CryptoProvider getCryptoProvider();
}
