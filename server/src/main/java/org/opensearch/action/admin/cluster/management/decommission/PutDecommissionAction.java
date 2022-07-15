/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.management.decommission;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

/**
 * Transport endpoint for adding exclusions to voting config
 *
 * @opensearch.internal
 */
public final class PutDecommissionAction extends ActionType<AcknowledgedResponse> {
    public static final PutDecommissionAction INSTANCE = new PutDecommissionAction();
    public static final String NAME = "cluster:admin/management/decommission";

    private PutDecommissionAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
