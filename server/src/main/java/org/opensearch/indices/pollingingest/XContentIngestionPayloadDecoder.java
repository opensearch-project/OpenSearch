/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IngestionPayloadDecoder;
import org.opensearch.index.IngestionPayloadDecoderFactory;
import org.opensearch.index.IngestionPayloadDecodingException;
import org.opensearch.index.Message;

import java.util.Map;

/**
 * Built-in decoder that parses JSON (or any XContent format) payloads.
 *
 * <p>This is the default decoder used when {@code index.ingestion_source.decoder_type} is
 * {@code xcontent} or is not set. It preserves the existing ingestion behavior.
 */
public class XContentIngestionPayloadDecoder implements IngestionPayloadDecoder {

    public static final XContentIngestionPayloadDecoder INSTANCE = new XContentIngestionPayloadDecoder();

    @Override
    public Map<String, Object> decode(Message<?> message) {
        try {
            BytesReference payload = new BytesArray((byte[]) message.getPayload());
            return XContentHelper.convertToMap(payload, false, MediaTypeRegistry.xContentType(payload)).v2();
        } catch (Exception e) {
            throw new IngestionPayloadDecodingException("Failed to decode XContent payload: " + e.getMessage(), e);
        }
    }

    /**
     * Factory for the built-in {@code xcontent} decoder. Registered under the name
     * {@code xcontent} in the {@link IngestionPayloadDecoderRegistry}.
     */
    public static class Factory implements IngestionPayloadDecoderFactory {

        public static final Factory INSTANCE = new Factory();

        @Override
        public void validate(Map<String, Object> settings) {
            if (!settings.isEmpty()) {
                throw new IllegalArgumentException(
                    "The [xcontent] decoder does not accept decoder_settings, but found: " + settings.keySet()
                );
            }
        }

        @Override
        public IngestionPayloadDecoder create(IndexMetadata indexMetadata, int shardId, Map<String, Object> settings) {
            return XContentIngestionPayloadDecoder.INSTANCE;
        }
    }
}
