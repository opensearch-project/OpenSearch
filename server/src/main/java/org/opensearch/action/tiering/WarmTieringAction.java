/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.tiering;

import org.opensearch.action.ActionType;

/**
 * Tiering action class to move index from hot to warm
 *
 * @opensearch.experimental
 */
public class WarmTieringAction extends ActionType<HotToWarmTieringResponse> {

    public static final WarmTieringAction INSTANCE = new WarmTieringAction();
    public static final String NAME = "indices:admin/tiering/warm";

    public WarmTieringAction() {
        super(NAME, HotToWarmTieringResponse::new);
    }
}
