/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.identity.Scope;

/**
 * Action type to put a new search pipeline
 *
 * @opensearch.internal
 */
public class PutSearchPipelineAction extends ActionType<AcknowledgedResponse> {
    public static final PutSearchPipelineAction INSTANCE = new PutSearchPipelineAction();
    public static final String NAME = "cluster:admin/search/pipeline/put";

    public PutSearchPipelineAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Index_ALL);
    }
}
