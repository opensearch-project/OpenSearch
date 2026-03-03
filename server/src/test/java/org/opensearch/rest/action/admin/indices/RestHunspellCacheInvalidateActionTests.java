/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateRequest;
import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestHunspellCacheInvalidateAction}
 */
public class RestHunspellCacheInvalidateActionTests extends OpenSearchTestCase {

    // ==================== Route Tests ====================

    public void testRoutes() {
        HunspellService mockService = mock(HunspellService.class);
        RestHunspellCacheInvalidateAction action = new RestHunspellCacheInvalidateAction(mockService);
        
        List<RestHunspellCacheInvalidateAction.Route> routes = action.routes();
        assertThat(routes, hasSize(3));
        
        // Verify GET route
        assertTrue(routes.stream().anyMatch(r -> 
            r.getMethod() == RestRequest.Method.GET && r.getPath().equals("/_hunspell/cache")));
        
        // Verify POST invalidate route
        assertTrue(routes.stream().anyMatch(r -> 
            r.getMethod() == RestRequest.Method.POST && r.getPath().equals("/_hunspell/cache/_invalidate")));
        
        // Verify POST invalidate_all route
        assertTrue(routes.stream().anyMatch(r -> 
            r.getMethod() == RestRequest.Method.POST && r.getPath().equals("/_hunspell/cache/_invalidate_all")));
    }

    public void testHandlerName() {
        HunspellService mockService = mock(HunspellService.class);
        RestHunspellCacheInvalidateAction action = new RestHunspellCacheInvalidateAction(mockService);
        assertThat(action.getName(), equalTo("hunspell_cache_invalidate_action"));
    }

    // ==================== Request Building Tests ====================

    public void testBuildRequestWithPackageIdOnly() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setPackageId("pkg-12345");
        
        assertThat(request.getPackageId(), equalTo("pkg-12345"));
        assertThat(request.getLocale(), nullValue());
        assertThat(request.getCacheKey(), nullValue());
        assertThat(request.isInvalidateAll(), equalTo(false));
        assertNull(request.validate());
    }

    public void testBuildRequestWithPackageIdAndLocale() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setPackageId("pkg-12345");
        request.setLocale("en_US");
        
        assertThat(request.getPackageId(), equalTo("pkg-12345"));
        assertThat(request.getLocale(), equalTo("en_US"));
        assertThat(request.getCacheKey(), nullValue());
        assertThat(request.isInvalidateAll(), equalTo(false));
        assertNull(request.validate());
    }

    public void testBuildRequestWithCacheKey() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setCacheKey("pkg-12345:en_US");
        
        assertThat(request.getPackageId(), nullValue());
        assertThat(request.getLocale(), nullValue());
        assertThat(request.getCacheKey(), equalTo("pkg-12345:en_US"));
        assertThat(request.isInvalidateAll(), equalTo(false));
        assertNull(request.validate());
    }

    public void testBuildRequestWithInvalidateAll() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setInvalidateAll(true);
        
        assertThat(request.getPackageId(), nullValue());
        assertThat(request.getLocale(), nullValue());
        assertThat(request.getCacheKey(), nullValue());
        assertThat(request.isInvalidateAll(), equalTo(true));
        assertNull(request.validate());
    }

    public void testRequestValidationFailsWhenNoParametersProvided() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        // No parameters set
        
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("package_id"));
    }

    // ==================== Request Serialization Tests ====================

    public void testRequestSerialization() throws IOException {
        HunspellCacheInvalidateRequest original = new HunspellCacheInvalidateRequest();
        original.setPackageId("pkg-test");
        original.setLocale("de_DE");
        original.setCacheKey("some-key");
        original.setInvalidateAll(false);
        
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        
        StreamInput in = out.bytes().streamInput();
        HunspellCacheInvalidateRequest deserialized = new HunspellCacheInvalidateRequest(in);
        
        assertThat(deserialized.getPackageId(), equalTo(original.getPackageId()));
        assertThat(deserialized.getLocale(), equalTo(original.getLocale()));
        assertThat(deserialized.getCacheKey(), equalTo(original.getCacheKey()));
        assertThat(deserialized.isInvalidateAll(), equalTo(original.isInvalidateAll()));
    }

    public void testRequestSerializationWithNullValues() throws IOException {
        HunspellCacheInvalidateRequest original = new HunspellCacheInvalidateRequest();
        original.setInvalidateAll(true);
        // packageId, locale, cacheKey are null
        
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        
        StreamInput in = out.bytes().streamInput();
        HunspellCacheInvalidateRequest deserialized = new HunspellCacheInvalidateRequest(in);
        
        assertThat(deserialized.getPackageId(), nullValue());
        assertThat(deserialized.getLocale(), nullValue());
        assertThat(deserialized.getCacheKey(), nullValue());
        assertThat(deserialized.isInvalidateAll(), equalTo(true));
    }

    // ==================== Response Tests ====================

    public void testResponseSerialization() throws IOException {
        HunspellCacheInvalidateResponse original = new HunspellCacheInvalidateResponse(
            true, 5, "pkg-123", "en_US", "pkg-123:en_US"
        );
        
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        
        StreamInput in = out.bytes().streamInput();
        HunspellCacheInvalidateResponse deserialized = new HunspellCacheInvalidateResponse(in);
        
        assertThat(deserialized.isAcknowledged(), equalTo(true));
        assertThat(deserialized.getInvalidatedCount(), equalTo(5));
        assertThat(deserialized.getPackageId(), equalTo("pkg-123"));
        assertThat(deserialized.getLocale(), equalTo("en_US"));
        assertThat(deserialized.getCacheKey(), equalTo("pkg-123:en_US"));
    }

    public void testResponseSerializationWithNullValues() throws IOException {
        HunspellCacheInvalidateResponse original = new HunspellCacheInvalidateResponse(
            true, 10, null, null, null
        );
        
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        
        StreamInput in = out.bytes().streamInput();
        HunspellCacheInvalidateResponse deserialized = new HunspellCacheInvalidateResponse(in);
        
        assertThat(deserialized.isAcknowledged(), equalTo(true));
        assertThat(deserialized.getInvalidatedCount(), equalTo(10));
        assertThat(deserialized.getPackageId(), nullValue());
        assertThat(deserialized.getLocale(), nullValue());
        assertThat(deserialized.getCacheKey(), nullValue());
    }

    public void testResponseToXContent() throws IOException {
        HunspellCacheInvalidateResponse response = new HunspellCacheInvalidateResponse(
            true, 3, "pkg-abc", "fr_FR", "pkg-abc:fr_FR"
        );
        
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = BytesReference.bytes(builder).utf8ToString();
        
        assertThat(json, containsString("\"acknowledged\":true"));
        assertThat(json, containsString("\"invalidated_count\":3"));
        assertThat(json, containsString("\"package_id\":\"pkg-abc\""));
        assertThat(json, containsString("\"locale\":\"fr_FR\""));
        assertThat(json, containsString("\"cache_key\":\"pkg-abc:fr_FR\""));
    }

    public void testResponseToXContentOmitsNullValues() throws IOException {
        HunspellCacheInvalidateResponse response = new HunspellCacheInvalidateResponse(
            true, 7, null, null, null
        );
        
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = BytesReference.bytes(builder).utf8ToString();
        
        assertThat(json, containsString("\"acknowledged\":true"));
        assertThat(json, containsString("\"invalidated_count\":7"));
        // Null values should NOT be present in output
        assertFalse(json.contains("package_id"));
        assertFalse(json.contains("locale"));
        assertFalse(json.contains("cache_key"));
    }

    // ==================== REST Parameter Parsing Tests ====================

    public void testRestParamsPackageIdOnly() {
        Map<String, String> params = new HashMap<>();
        params.put("package_id", "my-pkg");
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withParams(params)
            .withPath("/_hunspell/cache/_invalidate")
            .withMethod(RestRequest.Method.POST)
            .build();
        
        assertThat(restRequest.param("package_id"), equalTo("my-pkg"));
        assertThat(restRequest.param("locale"), nullValue());
        assertThat(restRequest.param("cache_key"), nullValue());
    }

    public void testRestParamsPackageIdAndLocale() {
        Map<String, String> params = new HashMap<>();
        params.put("package_id", "my-pkg");
        params.put("locale", "es_ES");
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withParams(params)
            .withPath("/_hunspell/cache/_invalidate")
            .withMethod(RestRequest.Method.POST)
            .build();
        
        assertThat(restRequest.param("package_id"), equalTo("my-pkg"));
        assertThat(restRequest.param("locale"), equalTo("es_ES"));
    }

    public void testRestParamsCacheKey() {
        Map<String, String> params = new HashMap<>();
        params.put("cache_key", "pkg-xyz:it_IT");
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withParams(params)
            .withPath("/_hunspell/cache/_invalidate")
            .withMethod(RestRequest.Method.POST)
            .build();
        
        assertThat(restRequest.param("cache_key"), equalTo("pkg-xyz:it_IT"));
    }

    public void testRestPathInvalidateAll() {
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withPath("/_hunspell/cache/_invalidate_all")
            .withMethod(RestRequest.Method.POST)
            .build();
        
        assertTrue(restRequest.path().endsWith("/_invalidate_all"));
    }

    // ==================== HunspellService Cache Key Helper Tests ====================

    public void testBuildPackageCacheKey() {
        String key = HunspellService.buildPackageCacheKey("pkg-123", "en_US");
        assertThat(key, equalTo("pkg-123:en_US"));
    }

    public void testIsPackageCacheKey() {
        assertTrue(HunspellService.isPackageCacheKey("pkg-123:en_US"));
        assertTrue(HunspellService.isPackageCacheKey("some-id:de_DE"));
        assertFalse(HunspellService.isPackageCacheKey("en_US"));
        assertFalse(HunspellService.isPackageCacheKey("simple_locale"));
    }

    // ==================== Action Name Test ====================

    public void testActionName() {
        assertThat(
            org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateAction.NAME, 
            equalTo("cluster:admin/hunspell/cache/clear")
        );
    }

    // ==================== Response Params Test ====================

    public void testResponseParams() {
        HunspellService mockService = mock(HunspellService.class);
        RestHunspellCacheInvalidateAction action = new RestHunspellCacheInvalidateAction(mockService);
        
        Set<String> params = action.responseParams();
        assertTrue(params.contains("package_id"));
        assertTrue(params.contains("cache_key"));
        assertTrue(params.contains("locale"));
        assertThat(params.size(), equalTo(3));
    }
}