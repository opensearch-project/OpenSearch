/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

/**
 * A factory for creating new {@link CodecService} instance
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface CodecServiceFactory {
    /**
     * Create new {@link CodecService} instance
     * @param config code service configuration
     * @return new {@link CodecService} instance
     */
    CodecService createCodecService(CodecServiceConfig config);
}
