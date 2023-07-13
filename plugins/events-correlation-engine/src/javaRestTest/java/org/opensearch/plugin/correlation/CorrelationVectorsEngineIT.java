/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Correlation Vectors Engine e2e tests
 */
public class CorrelationVectorsEngineIT extends OpenSearchRestTestCase {

    private static final int DIMENSION = 4;
    private static final String PROPERTIES_FIELD_NAME = "properties";
    private static final String TYPE_FIELD_NAME = "type";
    private static final String CORRELATION_VECTOR_TYPE = "correlation_vector";
    private static final String DIMENSION_FIELD_NAME = "dimension";
    private static final int M = 16;
    private static final int EF_CONSTRUCTION = 128;
    private static final String INDEX_NAME = "test-index-1";
    private static final Float[][] TEST_VECTORS = new Float[][] {
        { 1.0f, 1.0f, 1.0f, 1.0f },
        { 2.0f, 2.0f, 2.0f, 2.0f },
        { 3.0f, 3.0f, 3.0f, 3.0f } };
    private static final float[][] TEST_QUERY_VECTORS = new float[][] {
        { 1.0f, 1.0f, 1.0f, 1.0f },
        { 2.0f, 2.0f, 2.0f, 2.0f },
        { 3.0f, 3.0f, 3.0f, 3.0f } };
    private static final Map<VectorSimilarityFunction, Function<Float, Float>> VECTOR_SIMILARITY_TO_SCORE = Map.of(
        VectorSimilarityFunction.EUCLIDEAN,
        (similarity) -> 1 / (1 + similarity),
        VectorSimilarityFunction.DOT_PRODUCT,
        (similarity) -> (1 + similarity) / 2,
        VectorSimilarityFunction.COSINE,
        (similarity) -> (1 + similarity) / 2
    );

    /**
     * test the e2e storage and query layer of events-correlation-engine
     * @throws IOException IOException
     */
    @SuppressWarnings("unchecked")
    public void testQuery() throws IOException {
        String textField = "text-field";
        String luceneField = "lucene-field";
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD_NAME)
            .startObject(textField)
            .field(TYPE_FIELD_NAME, "text")
            .endObject()
            .startObject(luceneField)
            .field(TYPE_FIELD_NAME, CORRELATION_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, DIMENSION)
            .startObject("correlation_ctx")
            .field("similarityFunction", VectorSimilarityFunction.EUCLIDEAN.name())
            .startObject("parameters")
            .field("m", M)
            .field("ef_construction", EF_CONSTRUCTION)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String mapping = Strings.toString(builder);
        createTestIndexWithMappingJson(client(), INDEX_NAME, mapping, getCorrelationDefaultIndexSettings());

        for (int idx = 0; idx < TEST_VECTORS.length; ++idx) {
            addCorrelationDoc(
                INDEX_NAME,
                String.valueOf(idx + 1),
                List.of(textField, luceneField),
                List.of(java.util.UUID.randomUUID().toString(), TEST_VECTORS[idx])
            );
        }
        refreshAllIndices();
        Assert.assertEquals(TEST_VECTORS.length, getDocCount(INDEX_NAME));

        int k = 2;
        for (float[] query : TEST_QUERY_VECTORS) {

            String correlationQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"correlation\": {\n"
                + "      \"lucene-field\": {\n"
                + "        \"vector\": \n"
                + Arrays.toString(query)
                + "        ,\n"
                + "        \"k\": 2,\n"
                + "        \"boost\": 1\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            Response response = searchCorrelationIndex(INDEX_NAME, correlationQuery, k);
            Map<String, Object> responseBody = entityAsMap(response);
            Assert.assertEquals(2, ((List<Object>) ((Map<String, Object>) responseBody.get("hits")).get("hits")).size());
            @SuppressWarnings("unchecked")
            double actualScore1 = Double.parseDouble(
                ((List<Map<String, Object>>) ((Map<String, Object>) responseBody.get("hits")).get("hits")).get(0).get("_score").toString()
            );
            @SuppressWarnings("unchecked")
            double actualScore2 = Double.parseDouble(
                ((List<Map<String, Object>>) ((Map<String, Object>) responseBody.get("hits")).get("hits")).get(1).get("_score").toString()
            );
            @SuppressWarnings("unchecked")
            List<Float> hit1 = ((Map<String, List<Double>>) ((List<Map<String, Object>>) ((Map<String, Object>) responseBody.get("hits"))
                .get("hits")).get(0).get("_source")).get(luceneField).stream().map(Double::floatValue).collect(Collectors.toList());
            float[] resultVector1 = new float[hit1.size()];
            for (int i = 0; i < hit1.size(); ++i) {
                resultVector1[i] = hit1.get(i);
            }

            @SuppressWarnings("unchecked")
            List<Float> hit2 = ((Map<String, List<Double>>) ((List<Map<String, Object>>) ((Map<String, Object>) responseBody.get("hits"))
                .get("hits")).get(1).get("_source")).get(luceneField).stream().map(Double::floatValue).collect(Collectors.toList());
            float[] resultVector2 = new float[hit2.size()];
            for (int i = 0; i < hit2.size(); ++i) {
                resultVector2[i] = hit2.get(i);
            }

            double rawScore1 = VectorSimilarityFunction.EUCLIDEAN.compare(resultVector1, query);
            Assert.assertEquals(rawScore1, actualScore1, 0.0001);
            double rawScore2 = VectorSimilarityFunction.EUCLIDEAN.compare(resultVector2, query);
            Assert.assertEquals(rawScore2, actualScore2, 0.0001);
        }
    }

    /**
     * unhappy test for the e2e storage and query layer of events-correlation-engine with no index exist
     */
    public void testQueryWithNoIndexExist() {
        float[] query = new float[] { 1.0f, 1.0f, 1.0f, 1.0f };
        String correlationQuery = "{\n"
            + "  \"query\": {\n"
            + "    \"correlation\": {\n"
            + "      \"lucene-field\": {\n"
            + "        \"vector\": \n"
            + Arrays.toString(query)
            + "        ,\n"
            + "        \"k\": 2,\n"
            + "        \"boost\": 1\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        Exception ex = assertThrows(ResponseException.class, () -> { searchCorrelationIndex(INDEX_NAME, correlationQuery, 2); });
        String expectedMessage = String.format(Locale.ROOT, "no such index [%s]", INDEX_NAME);
        String actualMessage = ex.getMessage();
        Assert.assertTrue(actualMessage.contains(expectedMessage));
    }

    /**
     * unhappy test for the e2e storage and query layer of events-correlation-engine with wrong mapping
     */
    public void testQueryWithWrongMapping() throws IOException {
        String textField = "text-field";
        String luceneField = "lucene-field";
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD_NAME)
            .startObject(textField)
            .field(TYPE_FIELD_NAME, "text")
            .endObject()
            .startObject(luceneField)
            .field(TYPE_FIELD_NAME, CORRELATION_VECTOR_TYPE)
            .field("test", DIMENSION)
            .startObject("correlation_ctx")
            .field("similarityFunction", VectorSimilarityFunction.EUCLIDEAN.name())
            .startObject("parameters")
            .field("m", M)
            .field("ef_construction", EF_CONSTRUCTION)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String mapping = Strings.toString(builder);
        Exception ex = assertThrows(ResponseException.class, () -> {
            createTestIndexWithMappingJson(client(), INDEX_NAME, mapping, getCorrelationDefaultIndexSettings());
        });

        String expectedMessage = String.format(
            Locale.ROOT,
            "unknown parameter [test] on mapper [%s] of type [correlation_vector]",
            luceneField
        );
        String actualMessage = ex.getMessage();
        Assert.assertTrue(actualMessage.contains(expectedMessage));
    }

    private String createTestIndexWithMappingJson(RestClient client, String index, String mapping, Settings settings) throws IOException {
        Request request = new Request("PUT", "/" + index);
        String entity = "{\"settings\": " + Strings.toString(XContentType.JSON, settings);
        if (mapping != null) {
            entity = entity + ",\"mappings\" : " + mapping;
        }

        entity = entity + "}";
        if (!settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)) {
            expectSoftDeletesWarning(request, index);
        }

        request.setJsonEntity(entity);
        client.performRequest(request);
        return index;
    }

    private Settings getCorrelationDefaultIndexSettings() {
        return Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).put("index.correlation", true).build();
    }

    private void addCorrelationDoc(String index, String docId, List<String> fieldNames, List<Object> vectors) throws IOException {
        Request request = new Request("POST", "/" + index + "/_doc/" + docId + "?refresh=true");

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < fieldNames.size(); i++) {
            builder.field(fieldNames.get(i), vectors.get(i));
        }
        builder.endObject();

        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.CREATED, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private Response searchCorrelationIndex(String index, String correlationQuery, int resultSize) throws IOException {
        Request request = new Request("POST", "/" + index + "/_search");

        request.addParameter("size", Integer.toString(resultSize));
        request.addParameter("explain", Boolean.toString(true));
        request.addParameter("search_type", "query_then_fetch");
        request.setJsonEntity(correlationQuery);

        Response response = client().performRequest(request);
        Assert.assertEquals("Search failed", RestStatus.OK, restStatus(response));
        return response;
    }

    private int getDocCount(String index) throws IOException {
        Response response = makeRequest(
            client(),
            "GET",
            String.format(Locale.getDefault(), "/%s/_count", index),
            Collections.emptyMap(),
            null
        );
        Assert.assertEquals(RestStatus.OK, restStatus(response));
        return Integer.parseInt(entityAsMap(response).get("count").toString());
    }

    private Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        Header... headers
    ) throws IOException {
        Request request = new Request(method, endpoint);
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);

        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options.build());
        request.addParameters(params);
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    private RestStatus restStatus(Response response) {
        return RestStatus.fromCode(response.getStatusLine().getStatusCode());
    }
}
