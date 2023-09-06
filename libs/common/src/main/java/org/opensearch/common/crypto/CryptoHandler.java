/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;

/**
 * Crypto provider abstractions for encryption and decryption of data. Allows registering multiple providers
 * for defining different ways of encrypting or decrypting data.
 *
 * T - Encryption Metadata / CryptoContext
 * U - Parsed Encryption Metadata / CryptoContext
 */
@ExperimentalApi
public interface CryptoHandler<T, U> {

    /**
     * To initialise or create a new crypto metadata to be used in encryption. This is needed to set the context before
     * beginning encryption.
     *
     * @return crypto metadata instance
     */
    T initEncryptionMetadata();

    /**
     * To load crypto metadata to be used in encryption from content header.
     * Note that underlying information in the loaded metadata object is same as present in the object created during
     * encryption but object type may differ.
     *
     * @return crypto metadata instance used in decryption.
     */
    U loadEncryptionMetadata(EncryptedHeaderContentSupplier encryptedHeaderContentSupplier) throws IOException;

    /**
     * Few encryption algorithms have certain conditions on the unit of content to be encrypted. This requires the
     * content size to be re adjusted in order to fulfil these conditions for partial writes. If write requests for
     * encryption of a part of content do not fulfil these conditions then encryption fails or can result in corrupted
     * content depending on the algorithm used. This method exposes a means to re-adjust sizes of such writes.
     *
     * @param cryptoContext crypto metadata instance
     * @param contentSize Size of the raw content
     * @return Adjusted size of the content.
     */
    long adjustContentSizeForPartialEncryption(T cryptoContext, long contentSize);

    /**
     * Estimate length of the encrypted content. It should only be used to determine length of entire content after
     * encryption.
     *
     * @param cryptoContext crypto metadata instance consisting of encryption metadata used in encryption.
     * @param contentLength Size of the raw content
     * @return Calculated size of the encrypted content.
     */
    long estimateEncryptedLengthOfEntireContent(T cryptoContext, long contentLength);

    /**
     * For given encrypted content length, estimate the length of the decrypted content.
     * @param cryptoContext crypto metadata instance consisting of encryption metadata used in encryption.
     * @param contentLength Size of the encrypted content
     * @return Calculated size of the decrypted content.
     */
    long estimateDecryptedLength(U cryptoContext, long contentLength);

    /**
     * Wraps a raw InputStream with encrypting stream
     *
     * @param encryptionMetadata created earlier to set the crypto metadata.
     * @param stream Raw InputStream to encrypt
     * @return encrypting stream wrapped around raw InputStream.
     */
    InputStreamContainer createEncryptingStream(T encryptionMetadata, InputStreamContainer stream);

    /**
     * Provides encrypted stream for a raw stream emitted for a part of content.
     *
     * @param cryptoContext crypto metadata instance.
     * @param stream raw stream for which encrypted stream has to be created.
     * @param totalStreams Number of streams being used for the entire content.
     * @param streamIdx Index of the current stream.
     * @return Encrypted stream for the provided raw stream.
     */
    InputStreamContainer createEncryptingStreamOfPart(T cryptoContext, InputStreamContainer stream, int totalStreams, int streamIdx);

    /**
     * This method accepts an encrypted stream and provides a decrypting wrapper.
     * @param encryptingStream to be decrypted.
     * @return Decrypting wrapper stream
     */
    InputStream createDecryptingStream(InputStream encryptingStream);

    /**
     * This method creates a {@link DecryptedRangedStreamProvider} which provides a wrapped stream to decrypt the
     * underlying stream. This also provides adjusted range against the actual range which should be used for fetching
     * and supplying the encrypted content for decryption. Extra content outside the range is trimmed down and returned
     * by the decrypted stream.
     * For partial reads of encrypted content, few algorithms require the range of content to be adjusted for
     * successful decryption. Adjusted range may or may not be same as the provided range. If range is adjusted then
     * starting offset of resultant range can be lesser than the starting offset of provided range and end
     * offset can be greater than the ending offset of the provided range.
     *
     * @param cryptoContext crypto metadata instance.
     * @param startPosOfRawContent starting position in the raw/decrypted content
     * @param endPosOfRawContent ending position in the raw/decrypted content
     */
    DecryptedRangedStreamProvider createDecryptingStreamOfRange(U cryptoContext, long startPosOfRawContent, long endPosOfRawContent);
}
