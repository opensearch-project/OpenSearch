/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;

import java.util.Collections;
import java.util.List;

/**
 * Util class for building star tree
 *
 * @opensearch.experimental
 */
public class StarTreeUtils {

    private StarTreeUtils() {}

    public static final int ALL = -1;

    /**
     * The suffix appended to dimension field names in the Star Tree index.
     */
    public static final String DIMENSION_SUFFIX = "dim";

    /**
     * The suffix appended to metric field names in the Star Tree index.
     */
    public static final String METRIC_SUFFIX = "metric";

    /**
     * Returns the full field name for a dimension in the star-tree index.
     *
     * @param starTreeFieldName star-tree field name
     * @param dimensionName     name of the dimension
     * @return full field name for the dimension in the star-tree index
     */
    public static String fullyQualifiedFieldNameForStarTreeDimensionsDocValues(String starTreeFieldName, String dimensionName) {
        return starTreeFieldName + "_" + dimensionName + "_" + DIMENSION_SUFFIX;
    }

    /**
     * Returns the full field name for a metric in the star-tree index.
     *
     * @param starTreeFieldName star-tree field name
     * @param fieldName         name of the metric field
     * @param metricName        name of the metric
     * @return full field name for the metric in the star-tree index
     */
    public static String fullyQualifiedFieldNameForStarTreeMetricsDocValues(String starTreeFieldName, String fieldName, String metricName) {
        return MetricAggregatorInfo.toFieldName(starTreeFieldName, fieldName, metricName) + "_" + METRIC_SUFFIX;
    }

    /**
     * Get field infos from field names
     *
     * @param fields field names
     * @return field infos
     */
    public static FieldInfo[] getFieldInfoList(List<String> fields) {
        FieldInfo[] fieldInfoList = new FieldInfo[fields.size()];

        // field number is not really used. We depend on unique field names to get the desired iterator
        int fieldNumber = 0;

        for (String fieldName : fields) {
            fieldInfoList[fieldNumber] = getFieldInfo(fieldName, DocValuesType.SORTED_NUMERIC, fieldNumber);
            fieldNumber++;
        }
        return fieldInfoList;
    }

    /**
     * Get new field info instance for a given field name and field number
     * @param fieldName name of the field
     * @param docValuesType doc value type of the field
     * @param fieldNumber number of the field
     * @return new field info instance
     */
    public static FieldInfo getFieldInfo(String fieldName, DocValuesType docValuesType, int fieldNumber) {
        return new FieldInfo(
            fieldName,
            fieldNumber,
            false,
            false,
            true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
            docValuesType,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    /**
     * Get new field info instance for a given field name and field number.
     * It's a dummy field info to fetch doc id set iterators based on field name.
     * <p>
     * Actual field infos uses fieldNumberAcrossStarTrees parameter to achieve consistent
     * and unique field numbers across fields and across multiple star trees
     *
     * @param fieldName name of the field
     * @param docValuesType doc value type of the field
     * @return new field info instance
     */
    public static FieldInfo getFieldInfo(String fieldName, DocValuesType docValuesType) {
        return new FieldInfo(
            fieldName,
            0,
            false,
            false,
            true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
            docValuesType,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

}
