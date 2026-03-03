/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.hunspell;

import org.opensearch.action.ActionType;

/**
 * Action type for invalidating Hunspell dictionary cache.
 * 
 * This action requires cluster admin permissions when the security plugin is enabled.
 * The action name "cluster:admin/hunspell/cache/clear" maps to IAM policies for authorization.
 *
 * @opensearch.internal
 */
public class HunspellCacheInvalidateAction extends ActionType<HunspellCacheInvalidateResponse> {

    public static final HunspellCacheInvalidateAction INSTANCE = new HunspellCacheInvalidateAction();
    public static final String NAME = "cluster:admin/hunspell/cache/clear";

    private HunspellCacheInvalidateAction() {
        super(NAME, HunspellCacheInvalidateResponse::new);
    }
}