/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec.correlation950;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugin.correlation.core.index.CorrelationParamsContext;
import org.opensearch.plugin.correlation.core.index.mapper.VectorFieldMapper;
import org.opensearch.plugin.correlation.core.index.query.CorrelationQueryFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.opensearch.plugin.correlation.core.index.codec.BasePerFieldCorrelationVectorsFormat.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.plugin.correlation.core.index.codec.BasePerFieldCorrelationVectorsFormat.METHOD_PARAMETER_M;
import static org.opensearch.plugin.correlation.core.index.codec.CorrelationCodecVersion.V_9_5_0;

/**
 * Unit tests for custom correlation codec
 */
public class CorrelationCodecTests extends OpenSearchTestCase {

    private static final String FIELD_NAME_ONE = "test_vector_one";
    private static final String FIELD_NAME_TWO = "test_vector_two";

    /**
     * test correlation vector index
     * @throws Exception Exception
     */
    public void testCorrelationVectorIndex() throws Exception {
        Function<MapperService, PerFieldCorrelationVectorsFormat> perFieldCorrelationVectorsProvider =
            mapperService -> new PerFieldCorrelationVectorsFormat(Optional.of(mapperService));
        Function<PerFieldCorrelationVectorsFormat, Codec> correlationCodecProvider = (correlationVectorsFormat -> new CorrelationCodec(
            V_9_5_0.getDefaultCodecDelegate(),
            correlationVectorsFormat
        ));
        testCorrelationVectorIndex(correlationCodecProvider, perFieldCorrelationVectorsProvider);
    }

    private void testCorrelationVectorIndex(
        final Function<PerFieldCorrelationVectorsFormat, Codec> codecProvider,
        final Function<MapperService, PerFieldCorrelationVectorsFormat> perFieldCorrelationVectorsProvider
    ) throws Exception {
        final MapperService mapperService = mock(MapperService.class);
        final CorrelationParamsContext correlationParamsContext = new CorrelationParamsContext(
            VectorSimilarityFunction.EUCLIDEAN,
            Map.of(METHOD_PARAMETER_M, 16, METHOD_PARAMETER_EF_CONSTRUCTION, 256)
        );

        final VectorFieldMapper.CorrelationVectorFieldType mappedFieldType1 = new VectorFieldMapper.CorrelationVectorFieldType(
            FIELD_NAME_ONE,
            Map.of(),
            3,
            correlationParamsContext
        );
        final VectorFieldMapper.CorrelationVectorFieldType mappedFieldType2 = new VectorFieldMapper.CorrelationVectorFieldType(
            FIELD_NAME_TWO,
            Map.of(),
            2,
            correlationParamsContext
        );
        when(mapperService.fieldType(eq(FIELD_NAME_ONE))).thenReturn(mappedFieldType1);
        when(mapperService.fieldType(eq(FIELD_NAME_TWO))).thenReturn(mappedFieldType2);

        var perFieldCorrelationVectorsFormatSpy = spy(perFieldCorrelationVectorsProvider.apply(mapperService));
        final Codec codec = codecProvider.apply(perFieldCorrelationVectorsFormatSpy);

        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setCodec(codec);

        final FieldType luceneFieldType = KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.EUCLIDEAN);
        float[] array = { 1.0f, 3.0f, 4.0f };
        KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME_ONE, array, luceneFieldType);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        doc.add(vectorField);
        writer.addDocument(doc);
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        verify(perFieldCorrelationVectorsFormatSpy).getKnnVectorsFormatForField(eq(FIELD_NAME_ONE));

        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = CorrelationQueryFactory.create(
            new CorrelationQueryFactory.CreateQueryRequest("dummy", FIELD_NAME_ONE, new float[] { 1.0f, 0.0f, 0.0f }, 1, null, null)
        );

        assertEquals(1, searcher.count(query));

        reader.close();
        dir.close();
    }
}
