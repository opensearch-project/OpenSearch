/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec.correlation990;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugin.correlation.core.index.codec.BasePerFieldCorrelationVectorsFormat;

import java.util.Optional;

/**
 * Class to define the hyper-parameters m and ef_construction for insert and store of correlation vectors into HNSW graphs based lucene index.
 */
public class PerFieldCorrelationVectorsFormat extends BasePerFieldCorrelationVectorsFormat {

    /**
     * Parameterized ctor for PerFieldCorrelationVectorsFormat
     * @param mapperService mapper service
     */
    public PerFieldCorrelationVectorsFormat(final Optional<MapperService> mapperService) {
        super(
            mapperService,
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            Lucene99HnswVectorsFormat::new,
            Lucene99HnswVectorsFormat::new
        );
    }
}
