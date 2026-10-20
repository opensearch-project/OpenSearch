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

import static org.opensearch.storage.common.tiering.TieringUtils.H2W_TIERING_TYPE_KEY;

/**
 * REST handler for moving indices to the warm tier.
 */
public class RestHotToWarmTierAction extends RestBaseTierAction {

    private static final String TARGET_TIER = "warm";

    /** Constructs a new RestHotToWarmTierAction. */
    public RestHotToWarmTierAction() {
        super(TARGET_TIER);
    }

    @Override
    public String getName() {
        return "warm_tier_action";
    }

    @Override
    protected String getMigrationType() {
        return H2W_TIERING_TYPE_KEY;
    }

    @Override
    protected ActionType<AcknowledgedResponse> getTierAction() {
        return HotToWarmTierAction.INSTANCE;
    }
}
