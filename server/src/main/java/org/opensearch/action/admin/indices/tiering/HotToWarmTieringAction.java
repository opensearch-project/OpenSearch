/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Tiering action to move indices from hot to warm
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class HotToWarmTieringAction extends ActionType<HotToWarmTieringResponse> {

    public static final HotToWarmTieringAction INSTANCE = new HotToWarmTieringAction();
    public static final String NAME = "indices:admin/tier/hot_to_warm";

    private HotToWarmTieringAction() {
        super(NAME, HotToWarmTieringResponse::new);
    }
}
