/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer.action;

import org.opensearch.action.ActionType;

/**
 * ActionType descriptor for the Foyer cache stats action.
 *
 * <p>Registered in {@code BlockCacheFoyerPlugin.getActions()}.
 *
 * @opensearch.internal
 */
public class FoyerCacheStatsAction extends ActionType<FoyerCacheStatsResponse> {

    public static final FoyerCacheStatsAction INSTANCE = new FoyerCacheStatsAction();
    public static final String NAME = TransportFoyerCacheStatsAction.ACTION_NAME;

    private FoyerCacheStatsAction() {
        super(NAME, FoyerCacheStatsResponse::new);
    }
}
