/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CodecServiceFactory;

public class CustomCodecServiceFactory implements CodecServiceFactory {
    @Override
    public CodecService createCodecService(CodecServiceConfig config) {
        return new CustomCodecService(config.getMapperService(), config.getLogger());
    }
}
