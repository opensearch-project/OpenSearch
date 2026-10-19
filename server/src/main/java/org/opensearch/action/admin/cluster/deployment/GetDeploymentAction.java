/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.action.ActionType;

/**
 * Action type for getting deployment status.
 *
 * @opensearch.internal
 */
public class GetDeploymentAction extends ActionType<GetDeploymentResponse> {

    public static final GetDeploymentAction INSTANCE = new GetDeploymentAction();
    public static final String NAME = "cluster:monitor/deployment/get";

    private GetDeploymentAction() {
        super(NAME, GetDeploymentResponse::new);
    }
}
