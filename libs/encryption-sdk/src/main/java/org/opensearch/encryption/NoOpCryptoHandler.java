/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;

public class NoOpCryptoHandler implements CryptoHandler<Object, Object> {

    /**
     * No op - Initialises metadata store used in encryption.
     * @return crypto metadata object constructed with encryption metadata like data key pair, encryption algorithm, etc.
     */
    public Object initEncryptionMetadata() {
        return new Object();
    }

    /**
     * No op content size adjustment of length of a partial content used in partial encryption.
     *
     * @param cryptoContextObj stateful object for a request consisting of materials required in encryption.
     * @param streamSize Size of the stream to be adjusted.
     * @return Adjusted size of the stream.
     */
    public long adjustContentSizeForPartialEncryption(Object cryptoContextObj, long streamSize) {
        return streamSize;
    }

    /**
     * No op - Estimate length of the encrypted stream.
     *
     * @param cryptoMetadataObj crypto metadata instance
     * @param contentLength Size of the raw content
     * @return Calculated size of the encrypted stream for the provided raw stream.
     */
    public long estimateEncryptedLengthOfEntireContent(Object cryptoMetadataObj, long contentLength) {
        return contentLength;
    }

    /**
     * No op length estimation for a given content length.
     *
     * @param cryptoMetadataObj crypto metadata instance
     * @param contentLength Size of the encrypted content
     * @return Calculated size of the encrypted stream for the provided raw stream.
     */
    public long estimateDecryptedLength(Object cryptoMetadataObj, long contentLength) {
        return contentLength;
    }

    /**
     * No op encrypting stream wrapper.
     *
     * @param cryptoContextObj consists encryption metadata.
     * @param stream Raw InputStream to encrypt
     * @return encrypting stream wrapped around raw InputStream.
     */
    public InputStreamContainer createEncryptingStream(Object cryptoContextObj, InputStreamContainer stream) {
        return stream;
    }

    /**
     * No op encrypting stream provider for a part of content.
     *
     * @param cryptoContextObj stateful object for a request consisting of materials required in encryption.
     * @param stream raw stream for which encrypted stream has to be created.
     * @param totalStreams Number of streams being used for the entire content.
     * @param streamIdx Index of the current stream.
     * @return Encrypted stream for the provided raw stream.
     */
    public InputStreamContainer createEncryptingStreamOfPart(
        Object cryptoContextObj,
        InputStreamContainer stream,
        int totalStreams,
        int streamIdx
    ) {
        return stream;
    }

    /**
     *
     * @param encryptedHeaderContentSupplier Supplier used to fetch bytes from source for header creation
     * @return parsed encryption metadata object
     * @throws IOException if content fetch for header creation fails
     */
    public Object loadEncryptionMetadata(EncryptedHeaderContentSupplier encryptedHeaderContentSupplier) throws IOException {
        return new Object();
    }

    /**
     * No op decrypting stream provider.
     *
     * @param encryptedStream to be decrypted.
     * @return Decrypting wrapper stream
     */
    public InputStream createDecryptingStream(InputStream encryptedStream) {
        return encryptedStream;
    }

    /**
     * No Op decrypted stream range provider
     *
     * @param cryptoContext crypto metadata instance consisting of encryption metadata used in encryption.
     * @param startPosOfRawContent starting position in the raw/decrypted content
     * @param endPosOfRawContent ending position in the raw/decrypted content
     * @return stream provider for decrypted stream for the specified range of content including adjusted range
     */
    public DecryptedRangedStreamProvider createDecryptingStreamOfRange(
        Object cryptoContext,
        long startPosOfRawContent,
        long endPosOfRawContent
    ) {
        long[] range = { startPosOfRawContent, endPosOfRawContent };
        return new DecryptedRangedStreamProvider(range, (encryptedStream) -> encryptedStream);
    }

}
