/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionType;

public class SampleAction extends ActionType<SampleResponse> {

    public static final SampleAction INSTANCE = new SampleAction();
    private static final String NAME = "cluster:admin/opensearch/sample_rest";
    private SampleAction() {
        super(NAME, SampleResponse::new);
    }
}
