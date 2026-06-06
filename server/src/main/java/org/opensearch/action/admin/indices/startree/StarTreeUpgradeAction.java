/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Action type for the star tree upgrade operation.
 * Defines the action name and singleton instance used to register
 * and invoke the star tree upgrade transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeUpgradeAction extends ActionType<StarTreeUpgradeResponse> {

    public static final StarTreeUpgradeAction INSTANCE = new StarTreeUpgradeAction();
    public static final String NAME = "indices:admin/startree/upgrade";

    private StarTreeUpgradeAction() {
        super(NAME, StarTreeUpgradeResponse::new);
    }
}
