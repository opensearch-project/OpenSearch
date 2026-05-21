/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * ActionType definition for the catalog snapshot transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CatalogSnapshotActionType extends ActionType<CatalogSnapshotResponse> {

    public static final String NAME = "indices:monitor/composite/catalog_snapshot";
    public static final CatalogSnapshotActionType INSTANCE = new CatalogSnapshotActionType();

    private CatalogSnapshotActionType() {
        super(NAME, CatalogSnapshotResponse::new);
    }
}
