/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cryptospi;

import java.io.Closeable;

/**
 * Master key provider responsible for management of master keys.
 */
public interface MasterKeyProvider extends Closeable {

    /**
     * Returns data key pair
     * @return data key pair generated by master key.
     */
    DataKeyPair generateDataPair();

    /**
     * Returns decrpted key against the encrypted key.
     * @param encryptedKey Key to decrypt
     * @return Decrypted version of key.
     */
    byte[] decryptKey(byte[] encryptedKey);

    /**
     * Returns key id.
     * @return key id
     */
    String getKeyId();
}
