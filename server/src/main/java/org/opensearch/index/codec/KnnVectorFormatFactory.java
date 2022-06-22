/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene92.Lucene92Codec;
import org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsFormat;
import org.opensearch.index.mapper.DenseVectorFieldMapper;
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
        if (mappedFieldType != null && mappedFieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType) {
            return true;
        }
        return false;
    }

    private int getIntegerParam(Map<String, Object> methodParams, String name) {
        return (Integer) methodParams.get(name);
    }
}
