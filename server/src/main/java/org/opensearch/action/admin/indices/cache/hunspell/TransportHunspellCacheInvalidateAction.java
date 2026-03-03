/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.hunspell;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for Hunspell cache invalidation.
 * 
 * This action is authorized via the "cluster:admin/hunspell/cache/clear" permission.
 * When the OpenSearch Security plugin is enabled, only users with cluster admin
 * permissions can execute this action.
 *
 * @opensearch.internal
 */
public class TransportHunspellCacheInvalidateAction extends HandledTransportAction<HunspellCacheInvalidateRequest, HunspellCacheInvalidateResponse> {

    private final HunspellService hunspellService;

    @Inject
    public TransportHunspellCacheInvalidateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        HunspellService hunspellService
    ) {
        super(HunspellCacheInvalidateAction.NAME, transportService, actionFilters, HunspellCacheInvalidateRequest::new);
        this.hunspellService = hunspellService;
    }

    @Override
    protected void doExecute(Task task, HunspellCacheInvalidateRequest request, ActionListener<HunspellCacheInvalidateResponse> listener) {
        try {
            String packageId = request.getPackageId();
            String locale = request.getLocale();
            String cacheKey = request.getCacheKey();
            boolean invalidateAll = request.isInvalidateAll();

            int invalidatedCount = 0;
            String responseCacheKey = null;

            if (invalidateAll) {
                // Invalidate all cached dictionaries
                invalidatedCount = hunspellService.invalidateAllDictionaries();
            } else if (packageId != null && locale != null) {
                // Invalidate specific package + locale combination
                responseCacheKey = HunspellService.buildPackageCacheKey(packageId, locale);
                boolean invalidated = hunspellService.invalidateDictionary(responseCacheKey);
                invalidatedCount = invalidated ? 1 : 0;
            } else if (packageId != null) {
                // Invalidate all locales for a package
                invalidatedCount = hunspellService.invalidateDictionariesByPackage(packageId);
            } else if (cacheKey != null) {
                // Invalidate a specific cache key directly
                responseCacheKey = cacheKey;
                boolean invalidated = hunspellService.invalidateDictionary(cacheKey);
                invalidatedCount = invalidated ? 1 : 0;
            }

            listener.onResponse(new HunspellCacheInvalidateResponse(
                true,
                invalidatedCount,
                packageId,
                locale,
                responseCacheKey
            ));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}