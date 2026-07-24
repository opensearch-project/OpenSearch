/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionPayloadDecoder;
import org.opensearch.index.IngestionPayloadDecoderFactory;
import org.opensearch.index.IngestionPayloadDecodingException;
import org.opensearch.index.Message;

import java.util.Map;

/**
 * Built-in decoder that parses JSON (or any XContent format) payloads.
 *
 * <p>This is the default decoder used when {@code index.ingestion_source.decoder_type} is
 * {@code xcontent} or is not set.
 */
public class XContentIngestionPayloadDecoder implements IngestionPayloadDecoder {

    /**
     * The name this decoder is registered under in the {@link IngestionPayloadDecoderRegistry}.
     */
    public static final String NAME = "xcontent";

    @Override
    public Map<String, Object> decode(Message<?> message) {
        try {
            return IngestionUtils.getParsedPayloadMap((byte[]) message.getPayload());
        } catch (Exception e) {
            throw new IngestionPayloadDecodingException("Failed to decode XContent payload: " + e.getMessage(), e);
        }
    }

    /**
     * Factory for the built-in {@code xcontent} decoder. Registered under the name
     * {@link #NAME} in the {@link IngestionPayloadDecoderRegistry}.
     */
    public static class Factory implements IngestionPayloadDecoderFactory {

        public static final Factory INSTANCE = new Factory();

        @Override
        public void validate(Map<String, Object> settings) {
            if (!settings.isEmpty()) {
                throw new IllegalArgumentException(
                    "The [" + NAME + "] decoder does not accept decoder_settings, but found: " + settings.keySet()
                );
            }
        }

        @Override
        public IngestionPayloadDecoder create(IndexMetadata indexMetadata, int shardId, Map<String, Object> settings) {
            return new XContentIngestionPayloadDecoder();
        }
    }
}
