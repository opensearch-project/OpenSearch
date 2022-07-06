/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene92.Lucene92Codec;
import org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsFormat;
import org.opensearch.index.mapper.KnnAlgorithmContext;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;

import static org.opensearch.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_BEAM_WIDTH;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_MAX_CONNECTIONS;

/**
 * Factory that creates a {@link KnnVectorsFormat knn vector format} based on a mapping
 * configuration for the field.
 *
 * @opensearch.internal
 */
public class KnnVectorFormatFactory {

    private final MapperService mapperService;

    public KnnVectorFormatFactory(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    /**
     * Create KnnVectorsFormat with parameters specified in the field definition or return codec's default
     * Knn Vector Format if field is not of DenseVector type
     * @param field name of the field
     * @return KnnVectorFormat that is specific to a mapped field
     */
    public KnnVectorsFormat create(final String field) {
        final MappedFieldType mappedFieldType = mapperService.fieldType(field);
        if (isDenseVectorFieldType(mappedFieldType)) {
            final DenseVectorFieldType knnVectorFieldType = (DenseVectorFieldType) mappedFieldType;
            final KnnAlgorithmContext algorithmContext = knnVectorFieldType.getKnnContext().getKnnAlgorithmContext();
            final Map<String, Object> methodParams = algorithmContext.getParameters();
            int maxConnections = getIntegerParam(methodParams, HNSW_PARAMETER_MAX_CONNECTIONS);
            int beamWidth = getIntegerParam(methodParams, HNSW_PARAMETER_BEAM_WIDTH);
            final KnnVectorsFormat luceneHnswVectorsFormat = new Lucene92HnswVectorsFormat(maxConnections, beamWidth);
            return luceneHnswVectorsFormat;
        }
        return Lucene92Codec.getDefault().knnVectorsFormat();
    }

    private boolean isDenseVectorFieldType(final MappedFieldType mappedFieldType) {
        return mappedFieldType instanceof DenseVectorFieldType;
    }

    private int getIntegerParam(Map<String, Object> methodParams, String name) {
        return (Integer) methodParams.get(name);
    }
}
