/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Action type to get search pipelines
 *
 * @opensearch.internal
 */
public class GetSearchPipelineAction extends ActionType<GetSearchPipelineResponse> {
    public static final GetSearchPipelineAction INSTANCE = new GetSearchPipelineAction();
    public static final String NAME = "cluster:admin/search/pipeline/get";

    public GetSearchPipelineAction() {
        super(NAME, GetSearchPipelineResponse::new);
    }
}
