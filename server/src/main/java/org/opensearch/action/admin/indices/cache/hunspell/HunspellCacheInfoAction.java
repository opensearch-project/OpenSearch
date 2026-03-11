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
 * Action for retrieving Hunspell cache information.
 *
 * <p>This action requires "cluster:monitor/hunspell/cache" permission when security is enabled.
 *
 * @opensearch.internal
 */
public class HunspellCacheInfoAction extends ActionType<HunspellCacheInfoResponse> {

    public static final HunspellCacheInfoAction INSTANCE = new HunspellCacheInfoAction();
    public static final String NAME = "cluster:monitor/hunspell/cache";

    private HunspellCacheInfoAction() {
        super(NAME, HunspellCacheInfoResponse::new);
    }
}
