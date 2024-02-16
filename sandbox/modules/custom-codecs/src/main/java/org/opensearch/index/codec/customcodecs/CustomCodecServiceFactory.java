/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CodecServiceFactory;

/**
 * A factory for creating new {@link CodecService} instance
 */
public class CustomCodecServiceFactory implements CodecServiceFactory {

    /** Creates a new instance. */
    public CustomCodecServiceFactory() {}

    @Override
    public CodecService createCodecService(CodecServiceConfig config) {
        return new CustomCodecService(config);
    }
}
