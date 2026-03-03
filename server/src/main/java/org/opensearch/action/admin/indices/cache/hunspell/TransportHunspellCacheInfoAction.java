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

import java.util.HashSet;
import java.util.Set;

/**
 * Transport action for retrieving Hunspell cache information.
 * 
 * <p>Requires "cluster:monitor/hunspell/cache" permission when security is enabled.
 *
 * @opensearch.internal
 */
public class TransportHunspellCacheInfoAction extends HandledTransportAction<HunspellCacheInfoRequest, HunspellCacheInfoResponse> {

    private final HunspellService hunspellService;

    @Inject
    public TransportHunspellCacheInfoAction(
        TransportService transportService,
        ActionFilters actionFilters,
        HunspellService hunspellService
    ) {
        super(HunspellCacheInfoAction.NAME, transportService, actionFilters, HunspellCacheInfoRequest::new);
        this.hunspellService = hunspellService;
    }

    @Override
    protected void doExecute(Task task, HunspellCacheInfoRequest request, ActionListener<HunspellCacheInfoResponse> listener) {
        try {
            Set<String> cachedKeys = hunspellService.getCachedDictionaryKeys();
            
            Set<String> packageKeys = new HashSet<>();
            Set<String> localeKeys = new HashSet<>();
            
            for (String key : cachedKeys) {
                if (HunspellService.isPackageCacheKey(key)) {
                    packageKeys.add(key);
                } else {
                    localeKeys.add(key);
                }
            }
            
            HunspellCacheInfoResponse response = new HunspellCacheInfoResponse(
                cachedKeys.size(),
                packageKeys.size(),
                localeKeys.size(),
                packageKeys,
                localeKeys
            );
            
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}