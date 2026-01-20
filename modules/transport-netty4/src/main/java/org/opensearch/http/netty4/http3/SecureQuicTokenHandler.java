/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4.http3;

import org.opensearch.common.Randomness;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicTokenHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.NetUtil;

/**
 * Secure {@link QuicTokenHandler} which uses HMAC_SHA256.
 */
public class SecureQuicTokenHandler implements QuicTokenHandler {
    private static final int HMAC_KEY_LEN = 32;
    private static final int HMAC_TAG_LEN = 32;
    private static final String HMAC_SHA_256 = "HmacSHA256";

    private static final String SERVER_NAME = "opensearch-netty";
    private static final byte[] SERVER_NAME_BYTES = SERVER_NAME.getBytes(CharsetUtil.US_ASCII);
    private static final ByteBuf SERVER_NAME_BUFFER = Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(SERVER_NAME_BYTES)).asReadOnly();

    private static final int MAX_TOKEN_LEN = HMAC_TAG_LEN + Quic.MAX_CONN_ID_LEN + NetUtil.LOCALHOST6.getAddress().length
        + SERVER_NAME_BYTES.length;

    private final byte[] key;

    public SecureQuicTokenHandler() {
        this.key = new byte[HMAC_KEY_LEN];
        Randomness.createSecure().nextBytes(key);
    }

    @Override
    public boolean writeToken(ByteBuf out, ByteBuf dcid, InetSocketAddress address) {
        final byte[] addr = address.getAddress().getAddress();
        final byte[] buffer = new byte[HMAC_TAG_LEN + addr.length + dcid.readableBytes()];

        System.arraycopy(addr, 0, buffer, HMAC_TAG_LEN, addr.length);
        dcid.getBytes(dcid.readerIndex(), buffer, HMAC_TAG_LEN + addr.length, dcid.readableBytes());

        try {
            final Mac mac = Mac.getInstance(HMAC_SHA_256);
            mac.init(new SecretKeySpec(key, HMAC_SHA_256));
            mac.update(buffer, HMAC_TAG_LEN, addr.length + dcid.readableBytes());
            System.arraycopy(mac.doFinal(), 0, buffer, 0, HMAC_TAG_LEN);
        } catch (final InvalidKeyException | NoSuchAlgorithmException ex) {
            return false;
        }

        out.writeBytes(SERVER_NAME_BYTES).writeBytes(buffer);
        return true;
    }

    @Override
    public int validateToken(ByteBuf token, InetSocketAddress address) {
        final byte[] addr = address.getAddress().getAddress();
        final int minLength = SERVER_NAME_BYTES.length + HMAC_TAG_LEN + addr.length;
        if (token.readableBytes() <= minLength) {
            return -1;
        }

        if (!SERVER_NAME_BUFFER.equals(token.slice(0, SERVER_NAME_BYTES.length))) {
            return -1;
        }

        final ByteBuf tag = token.slice(SERVER_NAME_BYTES.length, HMAC_TAG_LEN);
        final int length = token.readableBytes() - HMAC_TAG_LEN - SERVER_NAME_BYTES.length;
        final ByteBuf payload = token.slice(SERVER_NAME_BYTES.length + HMAC_TAG_LEN, length);
        try {
            final Mac mac = Mac.getInstance(HMAC_SHA_256);
            mac.init(new SecretKeySpec(key, HMAC_SHA_256));
            for (int i = 0; i < payload.readableBytes(); ++i) {
                mac.update(payload.getByte(payload.readerIndex() + i));
            }

            final byte[] actual = new byte[tag.readableBytes()];
            tag.getBytes(tag.readerIndex(), actual, 0, tag.readableBytes());

            final byte[] expected = mac.doFinal();
            if (!MessageDigest.isEqual(actual, expected)) {
                return -1;
            }
        } catch (final InvalidKeyException | NoSuchAlgorithmException ex) {
            return -1;
        }

        return minLength;
    }

    @Override
    public int maxTokenLength() {
        return MAX_TOKEN_LEN;
    }
}
