/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

/**
 * REST handler for moving indices to the hot tier.
 * getMigrationType and getTierAction will be added in the implementation PR.
 */
public class RestWarmToHotTierAction extends RestBaseTierAction {

    /** Constructs a new RestWarmToHotTierAction. */
    public RestWarmToHotTierAction() {
        super("hot");
    }

    @Override
    public String getName() {
        return "hot_tier_action";
    }
}
