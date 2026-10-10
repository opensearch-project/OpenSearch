/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Action type for transitioning a deployment (drain/finish).
 *
 * @opensearch.internal
 */
public class TransitionDeploymentAction extends ActionType<AcknowledgedResponse> {

    public static final TransitionDeploymentAction INSTANCE = new TransitionDeploymentAction();
    public static final String NAME = "cluster:admin/deployment/transition";

    private TransitionDeploymentAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
