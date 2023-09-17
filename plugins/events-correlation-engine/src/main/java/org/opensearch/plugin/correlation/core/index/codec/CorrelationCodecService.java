/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec;

import org.apache.lucene.codecs.Codec;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;

/**
 * custom Correlation Codec Service
 *
 * @opensearch.internal
 */
public class CorrelationCodecService extends CodecService {

    private final MapperService mapperService;

    /**
     * Parameterized ctor for CorrelationCodecService
     * @param codecServiceConfig Generic codec service config
     */
    public CorrelationCodecService(CodecServiceConfig codecServiceConfig) {
        super(codecServiceConfig.getMapperService(), codecServiceConfig.getIndexSettings(), codecServiceConfig.getLogger());
        mapperService = codecServiceConfig.getMapperService();
    }

    @Override
    public Codec codec(String name) {
        return CorrelationCodecVersion.current().getCorrelationCodecSupplier().apply(super.codec(name), mapperService);
    }
}
