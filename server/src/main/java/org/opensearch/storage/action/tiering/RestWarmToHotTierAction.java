/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

import static org.opensearch.storage.common.tiering.TieringUtils.W2H_TIERING_TYPE_KEY;

/**
 * REST handler for moving indices to the hot tier.
 */
public class RestWarmToHotTierAction extends RestBaseTierAction {

    private static final String TARGET_TIER = "hot";

    /** Constructs a new RestWarmToHotTierAction. */
    public RestWarmToHotTierAction() {
        super(TARGET_TIER);
    }

    @Override
    public String getName() {
        return "hot_tier_action";
    }

    @Override
    protected String getMigrationType() {
        return W2H_TIERING_TYPE_KEY;
    }

    @Override
    protected ActionType<AcknowledgedResponse> getTierAction() {
        return WarmToHotTierAction.INSTANCE;
    }
}
