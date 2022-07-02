/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.mapper;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import org.opensearch.index.query.QueryShardContext;

import java.util.Arrays;
import java.util.Map;

import static org.opensearch.index.mapper.KnnAlgorithmContext.Method.HNSW;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_BEAM_WIDTH;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_MAX_CONNECTIONS;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {

    private static final String FIELD_NAME = "field";
    private static final float[] VECTOR = { 2.0f, 4.5f };

    private DenseVectorFieldType fieldType;

    @Before
    public void setup() throws Exception {
        KnnAlgorithmContext knnMethodContext = new KnnAlgorithmContext(
            HNSW,
            Map.of(HNSW_PARAMETER_MAX_CONNECTIONS, 10, HNSW_PARAMETER_BEAM_WIDTH, 100)
        );
        KnnContext knnContext = new KnnContext(Metric.L2, knnMethodContext);
        fieldType = new DenseVectorFieldType(FIELD_NAME, 1, knnContext);
    }

    public void testValueDisplay() {
        Object actualFloatArray = fieldType.valueForDisplay(VECTOR);
        assertTrue(actualFloatArray instanceof float[]);
        assertArrayEquals(VECTOR, (float[]) actualFloatArray, 0.0f);

        KnnContext knnContextDEfaultAlgorithmContext = new KnnContext(
            Metric.L2,
            KnnAlgorithmContextFactory.defaultContext(KnnAlgorithmContext.Method.HNSW)
        );
        MappedFieldType ftDefaultAlgorithmContext = new DenseVectorFieldType(FIELD_NAME, 1, knnContextDEfaultAlgorithmContext);
        Object actualFloatArrayDefaultAlgorithmContext = ftDefaultAlgorithmContext.valueForDisplay(VECTOR);
        assertTrue(actualFloatArrayDefaultAlgorithmContext instanceof float[]);
        assertArrayEquals(VECTOR, (float[]) actualFloatArrayDefaultAlgorithmContext, 0.0f);
    }

    public void testTermQueryNotSupported() {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> fieldType.termsQuery(Arrays.asList(VECTOR), context)
        );
        assertEquals(exception.getMessage(), "[term] queries are not supported on [dense_vector] fields.");
    }

    public void testPrefixQueryNotSupported() {
        UnsupportedOperationException ee = expectThrows(
            UnsupportedOperationException.class,
            () -> fieldType.prefixQuery("foo*", null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[prefix] queries are not supported on [dense_vector] fields.", ee.getMessage());
    }

    public void testRegexpQueryNotSupported() {
        UnsupportedOperationException ee = expectThrows(
            UnsupportedOperationException.class,
            () -> fieldType.regexpQuery("foo?", randomInt(10), 0, randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries are not supported on [dense_vector] fields.", ee.getMessage());
    }

    public void testWildcardQueryNotSupported() {
        UnsupportedOperationException ee = expectThrows(
            UnsupportedOperationException.class,
            () -> fieldType.wildcardQuery("valu*", null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries are not supported on [dense_vector] fields.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> fieldType.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, randomMockShardContext())
        );
        assertEquals("[fuzzy] queries are not supported on [dense_vector] fields.", e.getMessage());
    }
}
