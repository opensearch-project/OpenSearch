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
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class StarTreeUtilsTests extends OpenSearchTestCase {

    public void testFullyQualifiedFieldNameForStarTreeDimensionsDocValues() {
        String starTreeFieldName = "myStarTreeField";
        String dimensionName = "dimension1";
        String expectedFieldName = "myStarTreeField_dimension1_dim";

        String actualFieldName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeDimensionsDocValues(starTreeFieldName, dimensionName);
        assertEquals(expectedFieldName, actualFieldName);
    }

    public void testFullyQualifiedFieldNameForStarTreeMetricsDocValues() {
        String starTreeFieldName = "myStarTreeField";
        String fieldName = "myField";
        String metricName = "metric1";
        String expectedFieldName = "myStarTreeField_myField_metric1_metric";

        String actualFieldName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(starTreeFieldName, fieldName, metricName);
        assertEquals(expectedFieldName, actualFieldName);
    }

    public void testGetFieldInfoList() {
        List<String> fieldNames = Arrays.asList("field1", "field2", "field3");
        FieldInfo[] actualFieldInfos = StarTreeUtils.getFieldInfoList(fieldNames);
        for (int i = 0; i < fieldNames.size(); i++) {
            assertFieldInfos(actualFieldInfos[i], fieldNames.get(i), i);
        }
    }

    public void testGetFieldInfo() {
        String fieldName = UUID.randomUUID().toString();
        int fieldNumber = randomInt();
        assertFieldInfos(StarTreeUtils.getFieldInfo(fieldName, fieldNumber), fieldName, fieldNumber);

    }

    private void assertFieldInfos(FieldInfo actualFieldInfo, String fieldName, Integer fieldNumber) {
        assertEquals(fieldName, actualFieldInfo.name);
        assertEquals(fieldNumber, actualFieldInfo.number, 0);
        assertFalse(actualFieldInfo.hasVectorValues());
        assertTrue(actualFieldInfo.hasNorms());
        assertFalse(actualFieldInfo.hasVectors());
        assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, actualFieldInfo.getIndexOptions());
        assertEquals(DocValuesType.SORTED_NUMERIC, actualFieldInfo.getDocValuesType());
        assertEquals(-1, actualFieldInfo.getDocValuesGen());
        assertEquals(Collections.emptyMap(), actualFieldInfo.attributes());
        assertEquals(0, actualFieldInfo.getPointDimensionCount());
        assertEquals(0, actualFieldInfo.getPointIndexDimensionCount());
        assertEquals(0, actualFieldInfo.getPointNumBytes());
        assertEquals(0, actualFieldInfo.getVectorDimension());
        assertEquals(VectorEncoding.FLOAT32, actualFieldInfo.getVectorEncoding());
        assertEquals(VectorSimilarityFunction.EUCLIDEAN, actualFieldInfo.getVectorSimilarityFunction());
        assertFalse(actualFieldInfo.isSoftDeletesField());
    }

}
