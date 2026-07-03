/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * v1 PME footer key metadata serializer and parser.
 *
 * <p>The metadata is stored as compact UTF-8 JSON in Parquet {@code FileCryptoMetaData.key_metadata}.
 * It is plaintext bootstrap data — not secret — and is validated only after the file decrypts
 * successfully and AAD validation passes.
 *
 * <p>V1 JSON format (exact field order, no insignificant whitespace):
 * <pre>
 * {"version":1,"data_key_id":"default","message_id":"&lt;base64url-22-chars&gt;"}
 * </pre>
 *
 * <p>Readers must fail closed on: missing fields, malformed values, unknown versions,
 * unexpected fields, {@code data_key_id} != {@code "default"}, or {@code message_id} not
 * decoding to exactly 16 bytes.
 */
public final class PmeFileKeyMetadata {

    /** The only supported metadata version. */
    static final int V1 = 1;

    /** The only valid {@code data_key_id} in v1. */
    public static final String DEFAULT_DATA_KEY_ID = "default";

    private static final int MESSAGE_ID_BYTES = 16;

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private final int version;
    private final String dataKeyId;
    private final byte[] messageId; // 16 raw bytes

    private PmeFileKeyMetadata(int version, String dataKeyId, byte[] messageId) {
        this.version = version;
        this.dataKeyId = dataKeyId;
        this.messageId = messageId;
    }

    /**
     * Creates v1 metadata for a new Parquet file using the given random 16-byte message_id.
     *
     * @param messageId 16 random bytes generated per file
     * @return new v1 metadata instance
     */
    public static PmeFileKeyMetadata forNewFile(byte[] messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        if (messageId.length != MESSAGE_ID_BYTES) {
            throw new IllegalArgumentException("messageId must be 16 bytes, got: " + messageId.length);
        }
        return new PmeFileKeyMetadata(V1, DEFAULT_DATA_KEY_ID, messageId.clone());
    }

    /**
     * Serializes to compact UTF-8 JSON bytes in the exact v1 field order:
     * {@code version}, {@code data_key_id}, {@code message_id}.
     *
     * @return compact JSON bytes, safe to store in {@code FileCryptoMetaData.key_metadata}
     */
    public byte[] toJsonBytes() {
        String b64 = Base64.getUrlEncoder().withoutPadding().encodeToString(messageId);
        // Manual construction guarantees exact field order as required by the v1 spec.
        String json = "{\"version\":" + version
            + ",\"data_key_id\":\"" + dataKeyId
            + "\",\"message_id\":\"" + b64 + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses and validates v1 JSON bytes from a Parquet footer.
     * Rejects: missing fields, unknown versions, wrong {@code data_key_id}, malformed
     * {@code message_id}, and unknown JSON fields.
     *
     * @param jsonBytes raw bytes from {@code FileCryptoMetaData.key_metadata}
     * @return validated metadata instance
     * @throws IOException if the bytes are malformed, version is unsupported, or validation fails
     */
    public static PmeFileKeyMetadata parse(byte[] jsonBytes) throws IOException {
        Objects.requireNonNull(jsonBytes, "jsonBytes must not be null");
        if (jsonBytes.length == 0) {
            throw new IOException("PME key metadata is empty");
        }
        JsonRepr raw;
        try {
            raw = MAPPER.readValue(jsonBytes, JsonRepr.class);
        } catch (Exception e) {
            throw new IOException("Failed to parse PME key metadata JSON: " + e.getMessage(), e);
        }
        if (raw.version != V1) {
            throw new IOException("Unsupported PME metadata version: " + raw.version);
        }
        if (raw.dataKeyId == null || raw.dataKeyId.isEmpty()) {
            throw new IOException("PME metadata missing data_key_id");
        }
        if (DEFAULT_DATA_KEY_ID.equals(raw.dataKeyId) == false) {
            throw new IOException("PME v1 data_key_id must be 'default', got: " + raw.dataKeyId);
        }
        if (raw.messageId == null || raw.messageId.isEmpty()) {
            throw new IOException("PME metadata missing message_id");
        }
        byte[] messageIdBytes;
        try {
            messageIdBytes = Base64.getUrlDecoder().decode(raw.messageId);
        } catch (IllegalArgumentException e) {
            throw new IOException("PME metadata message_id is not valid base64url: " + e.getMessage(), e);
        }
        if (messageIdBytes.length != MESSAGE_ID_BYTES) {
            throw new IOException(
                "PME metadata message_id must decode to 16 bytes, got: " + messageIdBytes.length
            );
        }
        return new PmeFileKeyMetadata(raw.version, raw.dataKeyId, messageIdBytes);
    }

    public int version() {
        return version;
    }

    public String dataKeyId() {
        return dataKeyId;
    }

    /** Returns a defensive copy of the 16-byte message_id. */
    public byte[] messageId() {
        return messageId.clone();
    }

    /** Jackson POJO for deserialization only. */
    private static final class JsonRepr {
        public final int version;
        public final String dataKeyId;
        public final String messageId;

        @JsonCreator
        JsonRepr(
            @JsonProperty(value = "version", required = true) int version,
            @JsonProperty(value = "data_key_id", required = true) String dataKeyId,
            @JsonProperty(value = "message_id", required = true) String messageId
        ) {
            this.version = version;
            this.dataKeyId = dataKeyId;
            this.messageId = messageId;
        }
    }
}

