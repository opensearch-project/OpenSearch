/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.query;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.junit.Assert;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugin.correlation.core.index.mapper.VectorFieldMapper;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Correlation Query Builder
 */
public class CorrelationQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "myvector";
    private static final int K = 1;
    private static final TermQueryBuilder TERM_QUERY = QueryBuilders.termQuery("field", "value");
    private static final float[] QUERY_VECTOR = new float[] { 1.0f, 2.0f, 3.0f, 4.0f };

    /**
     * test invalid number of nearby neighbors
     */
    public void testInvalidK() {
        float[] queryVector = { 1.0f, 1.0f };

        expectThrows(IllegalArgumentException.class, () -> new CorrelationQueryBuilder(FIELD_NAME, queryVector, -K));
        expectThrows(IllegalArgumentException.class, () -> new CorrelationQueryBuilder(FIELD_NAME, queryVector, 0));
        expectThrows(
            IllegalArgumentException.class,
            () -> new CorrelationQueryBuilder(FIELD_NAME, queryVector, CorrelationQueryBuilder.K_MAX + 1)
        );
    }

    /**
     * test empty vector scenario
     */
    public void testEmptyVector() {
        final float[] queryVector = null;
        expectThrows(IllegalArgumentException.class, () -> new CorrelationQueryBuilder(FIELD_NAME, queryVector, 1));
        final float[] queryVector1 = new float[] {};
        expectThrows(IllegalArgumentException.class, () -> new CorrelationQueryBuilder(FIELD_NAME, queryVector1, 1));
    }

    /**
     * test serde with xcontent
     * @throws IOException IOException
     */
    public void testFromXContent() throws IOException {
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(correlationQueryBuilder.fieldName());
        builder.field(CorrelationQueryBuilder.VECTOR_FIELD.getPreferredName(), correlationQueryBuilder.vector());
        builder.field(CorrelationQueryBuilder.K_FIELD.getPreferredName(), correlationQueryBuilder.getK());
        builder.endObject();
        builder.endObject();
        XContentParser contentParser = createParser(builder);
        contentParser.nextToken();
        CorrelationQueryBuilder actualBuilder = CorrelationQueryBuilder.parse(contentParser);
        Assert.assertEquals(actualBuilder, correlationQueryBuilder);
    }

    /**
     * test serde with xcontent
     * @throws IOException IOException
     */
    public void testFromXContentFromString() throws IOException {
        String correlationQuery = "{\n"
            + "    \"myvector\" : {\n"
            + "      \"vector\" : [\n"
            + "        1.0,\n"
            + "        2.0,\n"
            + "        3.0,\n"
            + "        4.0\n"
            + "      ],\n"
            + "      \"k\" : 1,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "}";
        XContentParser contentParser = createParser(JsonXContent.jsonXContent, correlationQuery);
        contentParser.nextToken();
        CorrelationQueryBuilder actualBuilder = CorrelationQueryBuilder.parse(contentParser);
        Assert.assertEquals(correlationQuery.replace("\n", "").replace(" ", ""), Strings.toString(XContentType.JSON, actualBuilder));
    }

    /**
     * test serde with xcontent with filters
     * @throws IOException IOException
     */
    public void testFromXContentWithFilters() throws IOException {
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K, TERM_QUERY);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(correlationQueryBuilder.fieldName());
        builder.field(CorrelationQueryBuilder.VECTOR_FIELD.getPreferredName(), correlationQueryBuilder.vector());
        builder.field(CorrelationQueryBuilder.K_FIELD.getPreferredName(), correlationQueryBuilder.getK());
        builder.field(CorrelationQueryBuilder.FILTER_FIELD.getPreferredName(), correlationQueryBuilder.getFilter());
        builder.endObject();
        builder.endObject();
        XContentParser contentParser = createParser(builder);
        contentParser.nextToken();
        CorrelationQueryBuilder actualBuilder = CorrelationQueryBuilder.parse(contentParser);
        Assert.assertEquals(actualBuilder, correlationQueryBuilder);
    }

    /**
     * test conversion o KnnFloatVectorQuery logic
     * @throws IOException IOException
     */
    public void testDoToQuery() throws IOException {
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        VectorFieldMapper.CorrelationVectorFieldType mockCorrVectorField = mock(VectorFieldMapper.CorrelationVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockCorrVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(anyString())).thenReturn(mockCorrVectorField);
        KnnFloatVectorQuery query = (KnnFloatVectorQuery) correlationQueryBuilder.doToQuery(mockQueryShardContext);
        Assert.assertEquals(FIELD_NAME, query.getField());
        Assert.assertArrayEquals(QUERY_VECTOR, query.getTargetCopy(), 0.1f);
        Assert.assertEquals(K, query.getK());
    }

    /**
     * test conversion o KnnFloatVectorQuery logic with filter
     * @throws IOException IOException
     */
    public void testDoToQueryWithFilter() throws IOException {
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K, TERM_QUERY);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        VectorFieldMapper.CorrelationVectorFieldType mockCorrVectorField = mock(VectorFieldMapper.CorrelationVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockCorrVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(anyString())).thenReturn(mockCorrVectorField);
        KnnFloatVectorQuery query = (KnnFloatVectorQuery) correlationQueryBuilder.doToQuery(mockQueryShardContext);
        Assert.assertEquals(FIELD_NAME, query.getField());
        Assert.assertArrayEquals(QUERY_VECTOR, query.getTargetCopy(), 0.1f);
        Assert.assertEquals(K, query.getK());
        Assert.assertEquals(TERM_QUERY.toQuery(mockQueryShardContext), query.getFilter());
    }

    /**
     * test conversion o KnnFloatVectorQuery logic failure with invalid dimensions
     */
    public void testDoToQueryInvalidDimensions() {
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        VectorFieldMapper.CorrelationVectorFieldType mockCorrVectorField = mock(VectorFieldMapper.CorrelationVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockCorrVectorField.getDimension()).thenReturn(400);
        when(mockQueryShardContext.fieldMapper(anyString())).thenReturn(mockCorrVectorField);
        expectThrows(IllegalArgumentException.class, () -> correlationQueryBuilder.doToQuery(mockQueryShardContext));
    }

    /**
     * test conversion o KnnFloatVectorQuery logic failure with invalid field type
     */
    public void testDoToQueryInvalidFieldType() {
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        NumberFieldMapper.NumberFieldType mockCorrVectorField = mock(NumberFieldMapper.NumberFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockQueryShardContext.fieldMapper(anyString())).thenReturn(mockCorrVectorField);
        expectThrows(IllegalArgumentException.class, () -> correlationQueryBuilder.doToQuery(mockQueryShardContext));
    }

    /**
     * test serialization of Correlation Query Builder
     * @throws Exception
     */
    public void testSerialization() throws Exception {
        assertSerialization(Optional.empty());
        assertSerialization(Optional.of(TERM_QUERY));
    }

    private void assertSerialization(final Optional<QueryBuilder> queryBuilderOptional) throws IOException {
        final CorrelationQueryBuilder builder = queryBuilderOptional.isPresent()
            ? new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K, queryBuilderOptional.get())
            : new CorrelationQueryBuilder(FIELD_NAME, QUERY_VECTOR, K);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.CURRENT);
            output.writeNamedWriteable(builder);

            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry())) {
                in.setVersion(Version.CURRENT);
                final QueryBuilder deserializedQuery = in.readNamedWriteable(QueryBuilder.class);

                assertNotNull(deserializedQuery);
                assertTrue(deserializedQuery instanceof CorrelationQueryBuilder);
                final CorrelationQueryBuilder deserializedKnnQueryBuilder = (CorrelationQueryBuilder) deserializedQuery;
                assertEquals(FIELD_NAME, deserializedKnnQueryBuilder.fieldName());
                assertArrayEquals(QUERY_VECTOR, (float[]) deserializedKnnQueryBuilder.vector(), 0.0f);
                assertEquals(K, deserializedKnnQueryBuilder.getK());
                if (queryBuilderOptional.isPresent()) {
                    assertNotNull(deserializedKnnQueryBuilder.getFilter());
                    assertEquals(queryBuilderOptional.get(), deserializedKnnQueryBuilder.getFilter());
                } else {
                    assertNull(deserializedKnnQueryBuilder.getFilter());
                }
            }
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> list = ClusterModule.getNamedXWriteables();
        SearchPlugin.QuerySpec<?> spec = new SearchPlugin.QuerySpec<>(
            TermQueryBuilder.NAME,
            TermQueryBuilder::new,
            TermQueryBuilder::fromXContent
        );
        list.add(new NamedXContentRegistry.Entry(QueryBuilder.class, spec.getName(), (p, c) -> spec.getParser().fromXContent(p)));
        NamedXContentRegistry registry = new NamedXContentRegistry(list);
        return registry;
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        final List<NamedWriteableRegistry.Entry> entries = ClusterModule.getNamedWriteables();
        entries.add(
            new NamedWriteableRegistry.Entry(
                QueryBuilder.class,
                CorrelationQueryBuilder.NAME_FIELD.getPreferredName(),
                CorrelationQueryBuilder::new
            )
        );
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new));
        return new NamedWriteableRegistry(entries);
    }
}
