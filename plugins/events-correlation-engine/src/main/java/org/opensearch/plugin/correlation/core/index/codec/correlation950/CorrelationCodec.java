/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec.correlation950;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.opensearch.plugin.correlation.core.index.codec.CorrelationCodecVersion;

/**
 * Correlation Codec class
 *
 * @opensearch.internal
 */
public class CorrelationCodec extends FilterCodec {
    private static final CorrelationCodecVersion VERSION = CorrelationCodecVersion.V_9_5_0;
    private final PerFieldCorrelationVectorsFormat perFieldCorrelationVectorsFormat;

    /**
     * ctor for CorrelationCodec
     */
    public CorrelationCodec() {
        this(VERSION.getDefaultCodecDelegate(), VERSION.getPerFieldCorrelationVectorsFormat());
    }

    /**
     * Parameterized ctor for CorrelationCodec
     * @param delegate codec delegate
     * @param perFieldCorrelationVectorsFormat correlation vectors format
     */
    public CorrelationCodec(Codec delegate, PerFieldCorrelationVectorsFormat perFieldCorrelationVectorsFormat) {
        super(VERSION.getCodecName(), delegate);
        this.perFieldCorrelationVectorsFormat = perFieldCorrelationVectorsFormat;
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return perFieldCorrelationVectorsFormat;
    }
}
