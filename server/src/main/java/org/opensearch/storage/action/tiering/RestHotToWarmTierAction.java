/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

/**
 * REST handler for moving indices to the warm tier.
 * getMigrationType and getTierAction will be added in the implementation PR.
 */
public class RestHotToWarmTierAction extends RestBaseTierAction {

    /** Constructs a new RestHotToWarmTierAction. */
    public RestHotToWarmTierAction() {
        super("warm");
    }

    @Override
    public String getName() {
        return "warm_tier_action";
    }
}
