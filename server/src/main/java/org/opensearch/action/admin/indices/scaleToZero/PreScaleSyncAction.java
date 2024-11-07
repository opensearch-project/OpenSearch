/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scaleToZero;

import org.opensearch.action.ActionType;

public class PreScaleSyncAction extends ActionType<PreScaleSyncResponse> {
    public static final PreScaleSyncAction INSTANCE = new PreScaleSyncAction();
    public static final String NAME = "indices:admin/settings/pre_scale_sync";

    private PreScaleSyncAction() {
        super(NAME, PreScaleSyncResponse::new);
    }
}
