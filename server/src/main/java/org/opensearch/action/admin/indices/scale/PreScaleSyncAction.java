/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

public class PreScaleSyncAction extends ActionType<AcknowledgedResponse> {
    public static final PreScaleSyncAction INSTANCE = new PreScaleSyncAction();
    public static final String NAME = "indices:admin/scale";

    private PreScaleSyncAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
