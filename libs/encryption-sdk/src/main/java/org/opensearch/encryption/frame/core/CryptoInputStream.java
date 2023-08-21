/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except
 * in compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.opensearch.encryption.frame.core;

import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import com.amazonaws.encryptionsdk.internal.MessageCryptoHandler;

import java.io.IOException;
import java.io.InputStream;

import static com.amazonaws.encryptionsdk.internal.Utils.assertNonNull;

/**
 * A CryptoInputStream is a subclass of java.io.InputStream. It performs cryptographic
 * transformation of the bytes passing through it.
 *
 * <p>The CryptoInputStream wraps a provided InputStream object and performs cryptographic
 * transformation of the bytes read from the wrapped InputStream. It uses the cryptography handler
 * provided during construction to invoke methods that perform the cryptographic transformations.
 *
 * <p>In short, reading from the CryptoInputStream returns bytes that are the cryptographic
 * transformations of the bytes read from the wrapped InputStream.
 *
 * <p>For example, if the cryptography handler provides methods for decryption, the
 * CryptoInputStream will read ciphertext bytes from the wrapped InputStream, decrypt, and return
 * them as plaintext bytes.
 *
 * <p>This class adheres strictly to the semantics, especially the failure semantics, of its
 * ancestor class java.io.InputStream. This class overrides all the methods specified in its
 * ancestor class.
 *
 * <p>To instantiate an instance of this class, please see {@link AwsCrypto}.
 *
 * @param <K> The type of {@link MasterKey}s used to manipulate the data.
 */
public class CryptoInputStream<K extends MasterKey<K>> extends InputStream {
    private static final int MAX_READ_LEN = 4096;

    private byte[] outBytes_ = new byte[0];
    private int outStart_;
    private int outEnd_;
    private final InputStream inputStream_;
    private final MessageCryptoHandler cryptoHandler_;
    private boolean hasFinalCalled_;
    private boolean hasProcessBytesCalled_;
    private final boolean isLastPart_;

    /**
     * Constructs a CryptoInputStream that wraps the provided InputStream object. It performs
     * cryptographic transformation of the bytes read from the wrapped InputStream using the methods
     * provided in the provided CryptoHandler implementation.
     *
     * @param inputStream the inputStream object to be wrapped.
     * @param cryptoHandler the cryptoHandler implementation that provides the methods to use in
     *     performing cryptographic transformation of the bytes read from the inputStream.
     * @param isLastPart Whether the provided InputStream is the last stream of the content.
     */
    public CryptoInputStream(final InputStream inputStream, final MessageCryptoHandler cryptoHandler, final boolean isLastPart) {
        inputStream_ = Utils.assertNonNull(inputStream, "inputStream");
        cryptoHandler_ = Utils.assertNonNull(cryptoHandler, "cryptoHandler");
        isLastPart_ = isLastPart;
    }

    /**
     * Fill the output bytes by reading from the wrapped InputStream and processing it through the
     * crypto handler.
     *
     * @return the number of bytes processed and returned by the crypto handler.
     */
    private int fillOutBytes() throws IOException, BadCiphertextException {
        final byte[] inputStreamBytes = new byte[MAX_READ_LEN];

        final int readLen = inputStream_.read(inputStreamBytes);

        return processBytes(readLen, inputStreamBytes);
    }

    private int processBytes(int readLen, byte[] inputStreamBytes) throws BadCiphertextException {

        outStart_ = 0;

        int processedLen = -1;
        if (readLen < 0 && isLastPart_) {
            // Mark end of stream until doFinal returns something.
            processedLen = -1;

            if (!hasFinalCalled_) {
                int outOffset = 0;
                int outLen = 0;

                // Handle the case where processBytes() was never called before.
                // This happens with an empty file where the end of stream is
                // reached on the first read attempt. In this case,
                // processBytes() must be called so the header bytes are written
                // during encryption.
                if (!hasProcessBytesCalled_) {
                    outBytes_ = new byte[cryptoHandler_.estimateOutputSize(0)];
                    outLen += cryptoHandler_.processBytes(inputStreamBytes, 0, 0, outBytes_, outOffset).getBytesWritten();
                    outOffset += outLen;
                } else {
                    outBytes_ = new byte[cryptoHandler_.estimateFinalOutputSize()];
                }

                // Get final bytes.
                outLen += cryptoHandler_.doFinal(outBytes_, outOffset);
                processedLen = outLen;
                hasFinalCalled_ = true;
            }
        } else if (readLen > 0) {
            // process the read bytes.
            outBytes_ = new byte[cryptoHandler_.estimatePartialOutputSize(readLen)];
            processedLen = cryptoHandler_.processBytes(inputStreamBytes, 0, readLen, outBytes_, outStart_).getBytesWritten();
            hasProcessBytesCalled_ = true;
        }

        outEnd_ = processedLen;
        return processedLen;
    }

    /**
     * {@inheritDoc}
     *
     * @throws BadCiphertextException This is thrown only during decryption if b contains invalid or
     *     corrupt ciphertext.
     */
    @Override
    public int read(final byte[] b, final int off, final int len) throws IllegalArgumentException, IOException, BadCiphertextException {
        assertNonNull(b, "b");

        if (len < 0 || off < 0) {
            throw new IllegalArgumentException("Invalid values for offset: " + off + " and length: " + len);
        }

        if (b.length == 0 || len == 0) {
            return 0;
        }

        // fill the output bytes if there aren't any left to return.
        if ((outEnd_ - outStart_) <= 0) {
            int newBytesLen = 0;

            // Block until a byte is read or end of stream in the underlying
            // stream is reached.
            while (newBytesLen == 0) {
                newBytesLen = fillOutBytes();
            }
            if (newBytesLen < 0) {
                return -1;
            }
        }

        final int copyLen = Math.min((outEnd_ - outStart_), len);
        System.arraycopy(outBytes_, outStart_, b, off, copyLen);
        outStart_ += copyLen;

        return copyLen;
    }

    /**
     * {@inheritDoc}
     *
     * @throws BadCiphertextException This is thrown only during decryption if b contains invalid or
     *     corrupt ciphertext.
     */
    @Override
    public int read(final byte[] b) throws IllegalArgumentException, IOException, BadCiphertextException {
        return read(b, 0, b.length);
    }

    /**
     * {@inheritDoc}
     *
     * @throws BadCiphertextException if b contains invalid or corrupt ciphertext. This is thrown only
     *     during decryption.
     */
    @Override
    public int read() throws IOException, BadCiphertextException {
        final byte[] bArray = new byte[1];
        int result = 0;

        while (result == 0) {
            result = read(bArray, 0, 1);
        }

        if (result > 0) {
            return (bArray[0] & 0xFF);
        } else {
            return result;
        }
    }

    @Override
    public void close() throws IOException {
        inputStream_.close();
    }

    /** Returns metadata associated with the performed cryptographic operation. */
    @Override
    public int available() throws IOException {
        return (outBytes_.length + inputStream_.available());
    }

    /**
     * Sets an upper bound on the size of the input data. This method should be called before reading
     * any data from the stream. If this method is not called prior to reading any data, performance
     * may be reduced (notably, it will not be possible to cache data keys when encrypting).
     *
     * <p>Among other things, this size is used to enforce limits configured on the {@link
     * CachingCryptoMaterialsManager}.
     *
     * <p>If the input size set here is exceeded, an exception will be thrown, and the encyption or
     * decryption will fail.
     *
     * <p>If this method is called multiple times, the smallest bound will be used.
     *
     * @param size Maximum input size.
     */
    public void setMaxInputLength(long size) {
        cryptoHandler_.setMaxInputLength(size);
    }
}
