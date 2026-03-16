/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.hunspell;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for hunspell cache info and invalidation APIs.
 *
 * <p>Tests the full REST→Transport→HunspellService flow on a real cluster:
 * <ul>
 *   <li>GET /_hunspell/cache (HunspellCacheInfoAction)</li>
 *   <li>POST /_hunspell/cache/_invalidate (HunspellCacheInvalidateAction)</li>
 * </ul>
 */
@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class HunspellCacheIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Enable lazy loading so dictionaries are only loaded on-demand
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(HunspellService.HUNSPELL_LAZY_LOAD.getKey(), true).build();
    }

    // ==================== Cache Info Tests ====================

    public void testCacheInfoReturnsEmptyWhenNoDictionariesLoaded() {
        HunspellCacheInfoResponse response = client().execute(HunspellCacheInfoAction.INSTANCE, new HunspellCacheInfoRequest()).actionGet();

        assertThat(response.getTotalCachedCount(), equalTo(0));
        assertThat(response.getPackageBasedCount(), equalTo(0));
        assertThat(response.getTraditionalLocaleCount(), equalTo(0));
        assertTrue(response.getPackageBasedKeys().isEmpty());
        assertTrue(response.getTraditionalLocaleKeys().isEmpty());
    }

    // ==================== Cache Invalidation Tests ====================

    public void testInvalidateAllOnEmptyCache() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setInvalidateAll(true);

        HunspellCacheInvalidateResponse response = client().execute(HunspellCacheInvalidateAction.INSTANCE, request).actionGet();

        assertTrue(response.isAcknowledged());
        assertThat(response.getInvalidatedCount(), equalTo(0));
    }

    public void testInvalidateByPackageIdOnEmptyCache() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setPackageId("nonexistent-pkg");

        HunspellCacheInvalidateResponse response = client().execute(HunspellCacheInvalidateAction.INSTANCE, request).actionGet();

        assertTrue(response.isAcknowledged());
        assertThat(response.getInvalidatedCount(), equalTo(0));
        assertThat(response.getPackageId(), equalTo("nonexistent-pkg"));
    }

    public void testInvalidateByCacheKeyOnEmptyCache() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setCacheKey("nonexistent-key");

        HunspellCacheInvalidateResponse response = client().execute(HunspellCacheInvalidateAction.INSTANCE, request).actionGet();

        assertTrue(response.isAcknowledged());
        assertThat(response.getInvalidatedCount(), equalTo(0));
    }

    // ==================== Request Validation Tests ====================

    public void testInvalidateRequestValidationFailsWithNoParameters() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        // No parameters set — should fail validation

        assertNotNull(request.validate());
    }

    public void testInvalidateRequestValidationFailsWithConflictingParameters() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setInvalidateAll(true);
        request.setPackageId("some-pkg");

        assertNotNull(request.validate());
    }

    public void testInvalidateRequestValidationFailsWithEmptyPackageId() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setPackageId("   ");

        assertNotNull(request.validate());
    }

    public void testInvalidateRequestValidationPassesWithPackageIdOnly() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setPackageId("valid-pkg");

        assertNull(request.validate());
    }

    public void testInvalidateRequestValidationPassesWithPackageIdAndLocale() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setPackageId("valid-pkg");
        request.setLocale("en_US");

        assertNull(request.validate());
    }

    public void testInvalidateRequestValidationFailsWithLocaleWithoutPackageId() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setLocale("en_US");

        assertNotNull(request.validate());
    }

    // ==================== Response Schema Tests ====================

    public void testInvalidateResponseAlwaysIncludesAllFields() {
        HunspellCacheInvalidateRequest request = new HunspellCacheInvalidateRequest();
        request.setInvalidateAll(true);

        HunspellCacheInvalidateResponse response = client().execute(HunspellCacheInvalidateAction.INSTANCE, request).actionGet();

        // Response should always include these fields for consistent schema
        assertTrue(response.isAcknowledged());
        assertThat(response.getInvalidatedCount(), greaterThanOrEqualTo(0));
        // Null fields should still be accessible (consistent schema)
        assertNull(response.getPackageId());
        assertNull(response.getLocale());
        assertNull(response.getCacheKey());
    }

    public void testCacheInfoResponseSchema() {
        HunspellCacheInfoResponse response = client().execute(HunspellCacheInfoAction.INSTANCE, new HunspellCacheInfoRequest()).actionGet();

        // Verify response schema has all expected fields
        assertThat(response.getTotalCachedCount(), greaterThanOrEqualTo(0));
        assertThat(response.getPackageBasedCount(), greaterThanOrEqualTo(0));
        assertThat(response.getTraditionalLocaleCount(), greaterThanOrEqualTo(0));
        assertNotNull(response.getPackageBasedKeys());
        assertNotNull(response.getTraditionalLocaleKeys());
    }
}
