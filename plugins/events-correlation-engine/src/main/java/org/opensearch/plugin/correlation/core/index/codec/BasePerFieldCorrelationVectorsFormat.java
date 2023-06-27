/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugin.correlation.core.index.mapper.CorrelationVectorFieldMapper;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Class to define the hyper-parameters m and ef_construction for insert and store of correlation vectors into HNSW graphs based lucene index.
 *
 * @opensearch.internal
 */
public abstract class BasePerFieldCorrelationVectorsFormat extends PerFieldKnnVectorsFormat {
    /**
     * the hyper-parameters for constructing HNSW graphs.
     * https://lucene.apache.org/core/9_4_0/core/org/apache/lucene/util/hnsw/HnswGraph.html
     */
    public static final String METHOD_PARAMETER_M = "m";
    /**
     * the hyper-parameters for constructing HNSW graphs.
     * https://lucene.apache.org/core/9_4_0/core/org/apache/lucene/util/hnsw/HnswGraph.html
     */
    public static final String METHOD_PARAMETER_EF_CONSTRUCTION = "ef_construction";

    private final Optional<MapperService> mapperService;
    private final int defaultMaxConnections;
    private final int defaultBeamWidth;
    private final Supplier<KnnVectorsFormat> defaultFormatSupplier;
    private final BiFunction<Integer, Integer, KnnVectorsFormat> formatSupplier;

    /**
     * Parameterized ctor of BasePerFieldCorrelationVectorsFormat
     * @param mapperService mapper service
     * @param defaultMaxConnections default m
     * @param defaultBeamWidth default ef_construction
     * @param defaultFormatSupplier default format supplier
     * @param formatSupplier format supplier
     */
    public BasePerFieldCorrelationVectorsFormat(
        Optional<MapperService> mapperService,
        int defaultMaxConnections,
        int defaultBeamWidth,
        Supplier<KnnVectorsFormat> defaultFormatSupplier,
        BiFunction<Integer, Integer, KnnVectorsFormat> formatSupplier
    ) {
        this.mapperService = mapperService;
        this.defaultMaxConnections = defaultMaxConnections;
        this.defaultBeamWidth = defaultBeamWidth;
        this.defaultFormatSupplier = defaultFormatSupplier;
        this.formatSupplier = formatSupplier;
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        if (!isCorrelationVectorFieldType(field)) {
            return defaultFormatSupplier.get();
        }

        var type = (CorrelationVectorFieldMapper.CorrelationVectorFieldType) mapperService.orElseThrow(
            () -> new IllegalArgumentException(
                String.format(Locale.getDefault(), "Cannot read field type for field [%s] because mapper service is not available", field)
            )
        ).fieldType(field);

        var params = type.getCorrelationParams().getParameters();
        int maxConnections = getMaxConnections(params);
        int beamWidth = getBeamWidth(params);

        return formatSupplier.apply(maxConnections, beamWidth);
    }

    private boolean isCorrelationVectorFieldType(final String field) {
        return mapperService.isPresent()
            && mapperService.get().fieldType(field) instanceof CorrelationVectorFieldMapper.CorrelationVectorFieldType;
    }

    private int getMaxConnections(final Map<String, Object> params) {
        if (params != null && params.containsKey(METHOD_PARAMETER_M)) {
            return (int) params.get(METHOD_PARAMETER_M);
        }
        return defaultMaxConnections;
    }

    private int getBeamWidth(final Map<String, Object> params) {
        if (params != null && params.containsKey(METHOD_PARAMETER_EF_CONSTRUCTION)) {
            return (int) params.get(METHOD_PARAMETER_EF_CONSTRUCTION);
        }
        return defaultBeamWidth;
    }
}
