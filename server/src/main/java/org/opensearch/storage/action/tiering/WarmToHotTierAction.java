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
 * Action type for moving indices from warm to hot tier in OpenSearch's tiered storage.
 */
public class WarmToHotTierAction extends ActionType<AcknowledgedResponse> {

    /**
     * Singleton instance of the WarmToHotTierAction.
     */
    public static final WarmToHotTierAction INSTANCE = new WarmToHotTierAction();
    /**
     * Action name for warm to hot tier operations.
     */
    public static final String NAME = "indices:admin/_tier/warm_to_hot";

    private WarmToHotTierAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
