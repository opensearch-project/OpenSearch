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

/**
 * Action type for moving indices from hot to warm tier in OpenSearch's tiered storage.
 */
public class HotToWarmTierAction extends ActionType<AcknowledgedResponse> {

    /**
    * Singleton instance of the HotToWarmTierAction.
    */
    public static final HotToWarmTierAction INSTANCE = new HotToWarmTierAction();
    /**
     * Action name for hot to warm tier operations.
     */
    public static final String NAME = "indices:admin/_tier/hot_to_warm";

    private HotToWarmTierAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
