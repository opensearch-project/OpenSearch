/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Connector-private Azure metadata wire format. Keys use a case-insensitive owned prefix followed by lowercase hex of strict UTF-8,
 * and values use a version marker followed by canonical Base64 of strict UTF-8. Non-owned metadata is external and is not exposed.
 */
final class AzureBlobMetadataCodec {

    static final String KEY_PREFIX = "opensearch_metadata_v1_";
    static final int MAX_METADATA_SIZE = 8 * 1024;

    private static final String OWNED_KEY_PREFIX = "opensearch_metadata_";
    private static final String VALUE_PREFIX = "v1_";
    private static final String HEADER_PREFIX = "x-ms-meta-";
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private AzureBlobMetadataCodec() {}

    static Map<String, String> encode(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> encoded = new TreeMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("Azure blob metadata keys must not be null");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("Azure blob metadata values must not be null");
            }

            final String wireKey = KEY_PREFIX + toLowerHex(encodeUtf8(entry.getKey(), "key"));
            final String wireValue = VALUE_PREFIX + Base64.getEncoder().encodeToString(encodeUtf8(entry.getValue(), "value"));
            final String previous = encoded.put(wireKey, wireValue);
            if (previous != null) {
                throw new IllegalArgumentException("Azure blob metadata contains colliding logical keys");
            }
        }

        final int encodedSize = encodedMetadataSize(encoded);
        if (encodedSize > MAX_METADATA_SIZE) {
            throw new IllegalArgumentException(
                "Encoded Azure blob metadata is ["
                    + encodedSize
                    + "] bytes, which exceeds the conservative ["
                    + MAX_METADATA_SIZE
                    + "] byte limit"
            );
        }
        return Collections.unmodifiableMap(encoded);
    }

    static Map<String, String> decode(Map<String, String> metadata) throws IOException {
        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> decoded = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            final String wireKey = entry.getKey();
            if (wireKey == null || startsWithIgnoreCase(wireKey, OWNED_KEY_PREFIX) == false) {
                continue;
            }
            if (startsWithIgnoreCase(wireKey, KEY_PREFIX) == false) {
                throw new IOException("Unsupported or malformed OpenSearch Azure metadata key codec: [" + wireKey + "]");
            }

            final String suffix = wireKey.substring(KEY_PREFIX.length());
            final String logicalKey = decodeUtf8(fromLowerHex(suffix, wireKey), "key");
            final String logicalValue = decodeValue(entry.getValue(), wireKey);
            if (decoded.putIfAbsent(logicalKey, logicalValue) != null) {
                throw new IOException("Duplicate decoded OpenSearch Azure metadata key: [" + logicalKey + "]");
            }
        }
        return Collections.unmodifiableMap(decoded);
    }

    static int encodedMetadataSize(Map<String, String> encodedMetadata) {
        long size = 0;
        for (Map.Entry<String, String> entry : encodedMetadata.entrySet()) {
            // Count the complete ASCII HTTP header line as a conservative upper bound for Azure's aggregate metadata limit.
            size += HEADER_PREFIX.length();
            size += asciiLength(entry.getKey());
            size += 2; // ": "
            size += asciiLength(entry.getValue());
            size += 2; // CRLF
            if (size > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Encoded Azure blob metadata size exceeds the supported integer range");
            }
        }
        return (int) size;
    }

    private static String decodeValue(String wireValue, String wireKey) throws IOException {
        if (wireValue == null || wireValue.startsWith(VALUE_PREFIX) == false) {
            throw new IOException("Unsupported or malformed OpenSearch Azure metadata value codec for key [" + wireKey + "]");
        }

        final String payload = wireValue.substring(VALUE_PREFIX.length());
        final byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(payload);
        } catch (IllegalArgumentException e) {
            throw new IOException("Malformed Base64 OpenSearch Azure metadata value for key [" + wireKey + "]", e);
        }
        if (Base64.getEncoder().encodeToString(decoded).equals(payload) == false) {
            throw new IOException("Non-canonical Base64 OpenSearch Azure metadata value for key [" + wireKey + "]");
        }
        return decodeUtf8(decoded, "value");
    }

    private static byte[] encodeUtf8(String value, String field) {
        try {
            final ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(value));
            final byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("OpenSearch Azure metadata " + field + " is not valid Unicode", e);
        }
    }

    private static String decodeUtf8(byte[] value, String field) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(value))
                .toString();
        } catch (CharacterCodingException e) {
            throw new IOException("OpenSearch Azure metadata " + field + " is not valid UTF-8", e);
        }
    }

    private static String toLowerHex(byte[] value) {
        final char[] encoded = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            final int unsigned = value[i] & 0xff;
            encoded[i * 2] = HEX[unsigned >>> 4];
            encoded[i * 2 + 1] = HEX[unsigned & 0x0f];
        }
        return new String(encoded);
    }

    private static byte[] fromLowerHex(String value, String wireKey) throws IOException {
        if ((value.length() & 1) != 0) {
            throw new IOException("OpenSearch Azure metadata key has an odd-length hex suffix: [" + wireKey + "]");
        }
        final byte[] decoded = new byte[value.length() / 2];
        for (int i = 0; i < value.length(); i += 2) {
            final int high = lowerHexValue(value.charAt(i));
            final int low = lowerHexValue(value.charAt(i + 1));
            if (high < 0 || low < 0) {
                throw new IOException("OpenSearch Azure metadata key has a non-lowercase-hex suffix: [" + wireKey + "]");
            }
            decoded[i / 2] = (byte) ((high << 4) | low);
        }
        return decoded;
    }

    private static int lowerHexValue(char value) {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return value - 'a' + 10;
        }
        return -1;
    }

    private static int asciiLength(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Encoded Azure blob metadata must not contain null values");
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0x7f) {
                throw new IllegalArgumentException("Encoded Azure blob metadata must contain ASCII only");
            }
        }
        return value.length();
    }

    private static boolean startsWithIgnoreCase(String value, String prefix) {
        return value.length() >= prefix.length() && value.regionMatches(true, 0, prefix, 0, prefix.length());
    }
}
