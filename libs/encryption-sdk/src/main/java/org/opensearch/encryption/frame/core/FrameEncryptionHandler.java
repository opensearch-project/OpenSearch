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

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import com.amazonaws.encryptionsdk.internal.CryptoHandler;
import com.amazonaws.encryptionsdk.internal.ProcessingSummary;
import com.amazonaws.encryptionsdk.model.CipherFrameHeaders;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The frame encryption handler is a subclass of the encryption handler and thereby provides an
 * implementation of the Cryptography handler.
 *
 * <p>It implements methods for encrypting content and storing the encrypted bytes in frames.
 */
public class FrameEncryptionHandler implements CryptoHandler {
    private final SecretKey encryptionKey_;
    private final CryptoAlgorithm cryptoAlgo_;
    private final CipherHandler cipherHandler_;
    private final int nonceLen_;
    private final byte[] messageId_;
    private final int frameSize_;
    private final int tagLenBytes_;

    private long frameNumber_ = 1;
    private boolean isFinalFrame_;

    private final byte[] bytesToFrame_;
    private int bytesToFrameLen_;
    private boolean complete_ = false;

    /**
     * Construct an encryption handler for encrypting bytes and storing them in frames.
     *
     * @param encryptionKey Encryption key used to encrypt file content. Appended to file header.
     * @param nonceLen random number appended with every frame header.
     * @param cryptoAlgo Crypto algorithm used to perform encryption
     * @param messageId Message Id from the crypto header
     * @param frameSize Size of a frame.
     * @param frameStartNumber Number from which assignment has to start for new frames
     */
    public FrameEncryptionHandler(
        final SecretKey encryptionKey,
        final int nonceLen,
        final CryptoAlgorithm cryptoAlgo,
        final byte[] messageId,
        final int frameSize,
        final int frameStartNumber
    ) {
        encryptionKey_ = encryptionKey;
        cryptoAlgo_ = cryptoAlgo;
        nonceLen_ = nonceLen;
        messageId_ = messageId.clone();
        frameSize_ = frameSize;
        tagLenBytes_ = cryptoAlgo_.getTagLen();
        bytesToFrame_ = new byte[frameSize_];
        bytesToFrameLen_ = 0;
        cipherHandler_ = new CipherHandler(encryptionKey_, Cipher.ENCRYPT_MODE, cryptoAlgo_);
        frameNumber_ = frameStartNumber;
    }

    /**
     * Encrypt a block of bytes from in putting the plaintext result into out.
     *
     * <p>It encrypts by performing the following operations:
     *
     * <ol>
     *   <li>determine the size of encrypted content that can fit into current frame
     *   <li>call processBytes() of the underlying cipher to do corresponding cryptographic encryption
     *       of plaintext
     *   <li>check if current frame is fully filled using the processed bytes, write current frame to
     *       the output being returned.
     * </ol>
     *
     * @param in the input byte array.
     * @param out the output buffer the encrypted bytes go into.
     * @param outOff the offset into the output byte array the encrypted data starts at.
     * @return the number of bytes written to out and processed
     */
    @Override
    public ProcessingSummary processBytes(final byte[] in, final int off, final int len, final byte[] out, final int outOff)
        throws BadCiphertextException {
        int actualOutLen = 0;

        int size = len;
        int offset = off;
        while (size > 0) {

            final int currentFrameCapacity = frameSize_ - bytesToFrameLen_;
            // bind size to the capacity of the current frame
            size = Math.min(currentFrameCapacity, size);

            System.arraycopy(in, offset, bytesToFrame_, bytesToFrameLen_, size);
            bytesToFrameLen_ += size;

            // check if there is enough bytes to create a frame
            if (bytesToFrameLen_ == frameSize_) {
                actualOutLen += writeEncryptedFrame(bytesToFrame_, 0, bytesToFrameLen_, out, outOff + actualOutLen);

                // reset buffer len as a new frame is created in next iteration
                bytesToFrameLen_ = 0;
            }

            // update offset by the size of bytes being encrypted.
            offset += size;
            // update size to the remaining bytes starting at offset.
            size = len - offset;
        }

        return new ProcessingSummary(actualOutLen, len);
    }

    /**
     * Finish processing of the bytes by writing out the ciphertext or final frame if framing.
     *
     * @param out space for any resulting output data.
     * @param outOff offset into out to start copying the data at.
     * @return number of bytes written into out.
     */
    @Override
    public int doFinal(final byte[] out, final int outOff) throws BadCiphertextException {
        isFinalFrame_ = true;
        complete_ = true;
        return writeEncryptedFrame(bytesToFrame_, 0, bytesToFrameLen_, out, outOff);
    }

    /**
     * Return the size of the output buffer required for a processBytes plus a doFinal with an input
     * of inLen bytes.
     *
     * @param inLen the length of the input.
     * @return the space required to accommodate a call to processBytes and doFinal with len bytes of
     *     input.
     */
    @Override
    public int estimateOutputSize(final int inLen) {
        // include any bytes held for inclusion in a subsequent frame
        int totalContent = bytesToFrameLen_ + inLen;
        return (int) estimatePartialSizeFromMetadata(totalContent, true, frameSize_, nonceLen_, tagLenBytes_);
    }

    public static long estimatePartialSizeFromMetadata(
        long totalContent,
        boolean includeLastFrame,
        int frameSize,
        int nonceLen,
        int tagLenBytes
    ) {
        // compute the size of the frames that will be constructed
        long frames = totalContent / frameSize;
        long outSize = (frameSize * frames);

        // account for remaining data that will need a new frame.
        final long leftover = totalContent % frameSize;
        outSize += leftover;
        // even if leftover is 0, there will be a final frame.
        if (includeLastFrame || leftover > 0) {
            frames += 1;
        }

        /*
         * Calculate overhead of frame headers.
         */
        // nonce and MAC tag.
        outSize += frames * (nonceLen + tagLenBytes);

        // sequence number for all frames
        outSize += frames * (Integer.SIZE / Byte.SIZE);

        if (includeLastFrame) {
            // sequence number end for final frame
            outSize += Integer.SIZE / Byte.SIZE;

            // integer for storing final frame size
            outSize += Integer.SIZE / Byte.SIZE;
        }

        return outSize;
    }

    @Override
    public int estimatePartialOutputSize(int inLen) {
        int outSize = 0;
        int frames = 0;

        // include any bytes held for inclusion in a subsequent frame
        int totalContent = bytesToFrameLen_;
        if (inLen >= 0) {
            totalContent += inLen;
        }

        // compute the size of the frames that will be constructed
        frames = totalContent / frameSize_;
        outSize += (frameSize_ * frames);

        /*
         * Calculate overhead of frame headers.
         */
        // nonce and MAC tag.
        outSize += frames * (nonceLen_ + tagLenBytes_);

        // sequence number for all frames
        outSize += frames * (Integer.SIZE / Byte.SIZE);

        return outSize;
    }

    @Override
    public int estimateFinalOutputSize() {
        int outSize = 0;
        int frames = 0;

        // include any bytes held for inclusion in a subsequent frame
        int totalContent = bytesToFrameLen_;

        // compute the size of the frames that will be constructed
        frames = totalContent / frameSize_;
        outSize += (frameSize_ * frames);

        // account for remaining data that will need a new frame.
        final int leftover = totalContent % frameSize_;
        outSize += leftover;
        // even if leftover is 0, there will be a final frame.
        frames += 1;

        /*
         * Calculate overhead of frame headers.
         */
        // nonce and MAC tag.
        outSize += frames * (nonceLen_ + tagLenBytes_);

        // sequence number for all frames
        outSize += frames * (Integer.SIZE / Byte.SIZE);

        // sequence number end for final frame
        outSize += Integer.SIZE / Byte.SIZE;

        // integer for storing final frame size
        outSize += Integer.SIZE / Byte.SIZE;

        return outSize;
    }

    /**
     * We encrypt the bytes, create the headers for the block, and assemble the frame containing the
     * headers and the encrypted bytes.
     *
     * @param out the output buffer the encrypted bytes go into.
     * @param outOff the offset into the output byte array the encrypted data starts at.
     * @return the number of bytes written to out.
     * @throws BadCiphertextException thrown by the underlying cipher handler.
     * @throws AwsCryptoException if frame number exceeds the maximum allowed value.
     */
    private int writeEncryptedFrame(final byte[] input, final int off, final int len, final byte[] out, final int outOff)
        throws BadCiphertextException, AwsCryptoException {
        if (frameNumber_ > Constants.MAX_FRAME_NUMBER
            // Make sure we have the appropriate flag set for the final frame; we don't want to accept
            // non-final-frame data when there won't be a subsequent frame for it to go into.
            || (frameNumber_ == Constants.MAX_FRAME_NUMBER && !isFinalFrame_)) {
            throw new AwsCryptoException("Frame number exceeded the maximum allowed value.");
        }

        if (out.length == 0) {
            return 0;
        }

        int outLen = 0;

        byte[] contentAad;
        if (isFinalFrame_ == true) {
            contentAad = Utils.generateContentAad(messageId_, Constants.FINAL_FRAME_STRING_ID, (int) frameNumber_, len);
        } else {
            contentAad = Utils.generateContentAad(messageId_, Constants.FRAME_STRING_ID, (int) frameNumber_, frameSize_);
        }

        final byte[] nonce = getNonce();

        final byte[] encryptedBytes = cipherHandler_.cipherData(nonce, contentAad, input, off, len);

        // create the cipherblock headers now for the encrypted data
        final int encryptedContentLen = encryptedBytes.length - tagLenBytes_;
        final CipherFrameHeaders cipherFrameHeaders = new CipherFrameHeaders((int) frameNumber_, nonce, encryptedContentLen, isFinalFrame_);
        final byte[] cipherFrameHeaderBytes = cipherFrameHeaders.toByteArray();

        // assemble the headers and the encrypted bytes into a single block
        System.arraycopy(cipherFrameHeaderBytes, 0, out, outOff + outLen, cipherFrameHeaderBytes.length);
        outLen += cipherFrameHeaderBytes.length;
        System.arraycopy(encryptedBytes, 0, out, outOff + outLen, encryptedBytes.length);
        outLen += encryptedBytes.length;

        frameNumber_++;

        return outLen;
    }

    private byte[] getNonce() {
        /*
         * To mitigate the risk of IVs colliding within the same message, we use deterministic IV generation within a
         * message.
         */

        if (frameNumber_ < 1) {
            // This should never happen - however, since we use a "frame number zero" IV elsewhere (for
            // header auth),
            // we must be sure that we don't reuse it here.
            throw new IllegalStateException("Illegal frame number");
        }

        if ((int) frameNumber_ == Constants.ENDFRAME_SEQUENCE_NUMBER && !isFinalFrame_) {
            throw new IllegalStateException("Too many frames");
        }

        final byte[] nonce = new byte[nonceLen_];

        ByteBuffer buf = ByteBuffer.wrap(nonce);
        buf.order(ByteOrder.BIG_ENDIAN);
        // We technically only allocate the low 32 bits for the frame number, and the other bits are
        // defined to be
        // zero. However, since MAX_FRAME_NUMBER is 2^32-1, the high-order four bytes of the long will
        // be zero, so the
        // big-endian representation will also have zeros in that position.
        Utils.position(buf, buf.limit() - Long.BYTES);
        buf.putLong(frameNumber_);

        return nonce;
    }

    @Override
    public boolean isComplete() {
        return complete_;
    }
}
