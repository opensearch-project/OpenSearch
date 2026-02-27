/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action for Hunspell dictionary cache management and invalidation.
 * 
 * <h2>Endpoints:</h2>
 * <ul>
 *   <li>GET /_hunspell/cache - View all cached dictionary keys</li>
 *   <li>POST /_hunspell/cache/_invalidate?package_id=xxx - Invalidate all locales for a package</li>
 *   <li>POST /_hunspell/cache/_invalidate?cache_key=xxx - Invalidate a specific cache entry</li>
 *   <li>POST /_hunspell/cache/_invalidate_all - Invalidate all cached dictionaries</li>
 * </ul>
 * 
 * <h2>Cache Key Formats:</h2>
 * <ul>
 *   <li>Package-based: "{packageId}:{locale}" (e.g., "pkg-1234:en_US")</li>
 *   <li>Traditional: "{locale}" (e.g., "en_US")</li>
 * </ul>
 */
public class RestHunspellCacheInvalidateAction extends BaseRestHandler {

    private final HunspellService hunspellService;

    public RestHunspellCacheInvalidateAction(HunspellService hunspellService) {
        this.hunspellService = hunspellService;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                // GET to view cached keys
                new Route(GET, "/_hunspell/cache"),
                // POST to invalidate by package_id or cache_key
                new Route(POST, "/_hunspell/cache/_invalidate"),
                // POST to invalidate all
                new Route(POST, "/_hunspell/cache/_invalidate_all")
            )
        );
    }

    @Override
    public String getName() {
        return "hunspell_cache_invalidate_action";
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> params = new HashSet<>();
        params.add("package_id");
        params.add("cache_key");
        params.add("locale");
        return unmodifiableSet(params);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> {
            try {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();

                if (request.method() == RestRequest.Method.GET) {
                    // Return all cached dictionary keys with metadata
                    handleGetCacheInfo(builder);
                    
                } else if (request.path().endsWith("/_invalidate_all")) {
                    // Invalidate all cached dictionaries
                    handleInvalidateAll(builder);
                    
                } else if (request.path().endsWith("/_invalidate")) {
                    // Invalidate by package_id, cache_key, or package_id + locale
                    handleInvalidate(request, builder);
                }

                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                
            } catch (Exception e) {
                XContentBuilder errorBuilder = channel.newBuilder();
                errorBuilder.startObject();
                errorBuilder.field("error", e.getMessage());
                errorBuilder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, errorBuilder));
            }
        };
    }

    /**
     * Handles GET /_hunspell/cache - returns cache statistics and keys.
     */
    private void handleGetCacheInfo(XContentBuilder builder) throws IOException {
        Set<String> cachedKeys = hunspellService.getCachedDictionaryKeys();
        
        // Separate package-based and traditional keys for clarity
        Set<String> packageKeys = new HashSet<>();
        Set<String> localeKeys = new HashSet<>();
        
        for (String key : cachedKeys) {
            if (HunspellService.isPackageCacheKey(key)) {
                packageKeys.add(key);
            } else {
                localeKeys.add(key);
            }
        }
        
        builder.field("total_cached_count", cachedKeys.size());
        builder.field("package_based_count", packageKeys.size());
        builder.field("traditional_locale_count", localeKeys.size());
        builder.array("package_based_keys", packageKeys.toArray(new String[0]));
        builder.array("traditional_locale_keys", localeKeys.toArray(new String[0]));
        builder.field("usage", "POST /_hunspell/cache/_invalidate?package_id=pkg-1234 or ?cache_key=pkg-1234:en_US");
    }

    /**
     * Handles POST /_hunspell/cache/_invalidate_all - clears entire cache.
     */
    private void handleInvalidateAll(XContentBuilder builder) throws IOException {
        int count = hunspellService.invalidateAllDictionaries();
        builder.field("acknowledged", true);
        builder.field("invalidated_count", count);
    }

    /**
     * Handles POST /_hunspell/cache/_invalidate - invalidate specific entries.
     * 
     * Parameters:
     * - package_id: Invalidate all locales for a package (e.g., "pkg-1234")
     * - locale: (Optional) When combined with package_id, invalidate specific locale
     * - cache_key: Invalidate a specific cache key directly (e.g., "pkg-1234:en_US" or "en_US")
     */
    private void handleInvalidate(RestRequest request, XContentBuilder builder) throws IOException {
        String packageId = request.param("package_id");
        String locale = request.param("locale");
        String cacheKey = request.param("cache_key");
        
        if (packageId != null && locale != null) {
            // Invalidate specific package + locale combination
            String key = HunspellService.buildPackageCacheKey(packageId, locale);
            boolean invalidated = hunspellService.invalidateDictionary(key);
            builder.field("acknowledged", true);
            builder.field("package_id", packageId);
            builder.field("locale", locale);
            builder.field("cache_key", key);
            builder.field("invalidated", invalidated);
            
        } else if (packageId != null) {
            // Invalidate all locales for a package
            int count = hunspellService.invalidateDictionariesByPackage(packageId);
            builder.field("acknowledged", true);
            builder.field("package_id", packageId);
            builder.field("invalidated_count", count);
            
        } else if (cacheKey != null) {
            // Invalidate a specific cache key directly
            boolean invalidated = hunspellService.invalidateDictionary(cacheKey);
            builder.field("acknowledged", true);
            builder.field("cache_key", cacheKey);
            builder.field("invalidated", invalidated);
            
        } else {
            builder.field("error", "Please provide 'package_id', 'cache_key', or 'package_id' + 'locale' parameters");
            builder.field("examples", new String[]{
                "?package_id=pkg-1234 (invalidate all locales in package)",
                "?package_id=pkg-1234&locale=en_US (invalidate specific locale)",
                "?cache_key=pkg-1234:en_US (invalidate by exact cache key)",
                "?cache_key=en_US (invalidate traditional locale)"
            });
        }
    }
}
