/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateAction;
import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

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
 *   <li>GET /_hunspell/cache - View all cached dictionary keys (read-only, no auth required)</li>
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
 * 
 * <h2>Authorization:</h2>
 * <p>POST operations require "cluster:admin/hunspell/cache/clear" permission when security is enabled.</p>
 */
public class RestHunspellCacheInvalidateAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestHunspellCacheInvalidateAction.class);
    private final HunspellService hunspellService;

    public RestHunspellCacheInvalidateAction(HunspellService hunspellService) {
        this.hunspellService = hunspellService;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                // GET to view cached keys (read-only, no auth required)
                new Route(GET, "/_hunspell/cache"),
                // POST to invalidate by package_id or cache_key (requires cluster admin)
                new Route(POST, "/_hunspell/cache/_invalidate"),
                // POST to invalidate all (requires cluster admin)
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
        // GET is read-only and doesn't go through TransportAction
        if (request.method() == RestRequest.Method.GET) {
            return channel -> {
                try {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    handleGetCacheInfo(builder);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } catch (Exception e) {
                    logger.error("Failed to get hunspell cache info", e);
                    XContentBuilder errorBuilder = channel.newBuilder();
                    errorBuilder.startObject();
                    errorBuilder.field("error", "Failed to retrieve cache information");
                    errorBuilder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, errorBuilder));
                }
            };
        }

        // POST operations go through TransportAction for authorization
        HunspellCacheInvalidateRequest invalidateRequest = buildInvalidateRequest(request);
        
        return channel -> client.execute(
            HunspellCacheInvalidateAction.INSTANCE,
            invalidateRequest,
            new RestToXContentListener<>(channel)
        );
    }

    /**
     * Build the transport request from REST parameters.
     */
    private HunspellCacheInvalidateRequest buildInvalidateRequest(RestRequest request) {
        HunspellCacheInvalidateRequest invalidateRequest = new HunspellCacheInvalidateRequest();
        
        if (request.path().endsWith("/_invalidate_all")) {
            invalidateRequest.setInvalidateAll(true);
        } else {
            String packageId = request.param("package_id");
            String locale = request.param("locale");
            String cacheKey = request.param("cache_key");
            
            invalidateRequest.setPackageId(packageId);
            invalidateRequest.setLocale(locale);
            invalidateRequest.setCacheKey(cacheKey);
        }
        
        return invalidateRequest;
    }

    /**
     * Handles GET /_hunspell/cache - returns cache statistics and keys.
     * This is a read-only operation that doesn't require authorization.
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
}