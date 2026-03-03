/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInfoAction;
import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInfoRequest;
import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateAction;
import org.opensearch.action.admin.indices.cache.hunspell.HunspellCacheInvalidateRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
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
 *   <li>GET /_hunspell/cache - View all cached dictionary keys (requires cluster:monitor/hunspell/cache permission)</li>
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
 * <ul>
 *   <li>GET operations require "cluster:monitor/hunspell/cache" permission when security is enabled.</li>
 *   <li>POST operations require "cluster:admin/hunspell/cache/clear" permission when security is enabled.</li>
 * </ul>
 */
public class RestHunspellCacheInvalidateAction extends BaseRestHandler {

    public RestHunspellCacheInvalidateAction() {
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                // GET to view cached keys (requires cluster:monitor/hunspell/cache permission)
                new Route(GET, "/_hunspell/cache"),
                // POST to invalidate by package_id or cache_key (requires cluster:admin/hunspell/cache/clear)
                new Route(POST, "/_hunspell/cache/_invalidate"),
                // POST to invalidate all (requires cluster:admin/hunspell/cache/clear)
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
        // GET goes through TransportAction for authorization
        if (request.method() == RestRequest.Method.GET) {
            HunspellCacheInfoRequest infoRequest = new HunspellCacheInfoRequest();
            return channel -> client.execute(
                HunspellCacheInfoAction.INSTANCE,
                infoRequest,
                new RestToXContentListener<>(channel)
            );
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
}