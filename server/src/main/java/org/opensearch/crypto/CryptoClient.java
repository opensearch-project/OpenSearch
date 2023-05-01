/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.opensearch.common.Stream;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.RefCounted;

import java.io.InputStream;

/**
 * Crypto plugin interface used for encryption and decryption.
 */
public interface CryptoClient extends RefCounted {

    /**
     * A factory interface for constructing crypto client.
     *
     */
    interface Factory {

        /**
         * Constructs a crypto client used for encryption and decryption
         *
         * @param cryptoSettings Settings needed for creating crypto client.
         * @param keyProviderName Name of the key provider.
         * @return instance of CryptoClient
         */
        CryptoClient create(Settings cryptoSettings, String keyProviderName);
    }

    /**
     * @return key provider type
     */
    String type();

    /**
     * @return key provider name
     */
    String name();

    /**
     * To Initialise a crypto context used in encryption. This might be needed to set the context before beginning
     * encryption.
     *
     * @return crypto context instance
     */
    Object initCryptoContext();

    /**
     * In scenarios where content is divided into multiple parts and streams are emitted against each part,
     * it is sometimes required to adjust the size of a part.
     *
     * @param cryptoContextObj crypto context instance
     * @param streamSize Size of the raw stream
     * @return Adjusted size of the stream.
     */
    long adjustStreamSize(Object cryptoContextObj, long streamSize);

    /**
     * Wraps a raw InputStream with encrypting stream
     *
     * @param cryptoContext created earlier to set the crypto context.
     * @param stream Raw InputStream to encrypt
     * @return encrypting stream wrapped around raw InputStream.
     */
    Stream createEncryptingStream(Object cryptoContext, Stream stream);

    /**
     * Provides encrypted stream for a raw stream emitted for a part of content.
     *
     * @param cryptoContextObj crypto context instance.
     * @param stream raw stream for which encrypted stream has to be created.
     * @param totalStreams Number of streams being used for the entire content.
     * @param streamIdx Index of the current stream.
     * @return Encrypted stream for the provided raw stream.
     */
    Stream createEncryptingStreamOfPart(Object cryptoContextObj, Stream stream, int totalStreams, int streamIdx);

    /**
     * This method accepts an encrypted stream and provides a decrypting wrapper.
     * @param encryptingStream to be decrypted.
     * @return Decrypting wrapper stream
     */
    InputStream createDecryptingStream(InputStream encryptingStream);
}
