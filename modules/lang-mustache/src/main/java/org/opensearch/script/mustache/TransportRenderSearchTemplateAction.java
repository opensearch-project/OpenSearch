/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script.mustache;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.script.ScriptService;
import org.opensearch.transport.TransportService;

public class TransportRenderSearchTemplateAction extends TransportSearchTemplateAction {

    @Inject
    public TransportRenderSearchTemplateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client
    ) {
        super(RenderSearchTemplateAction.NAME, transportService, actionFilters, scriptService, xContentRegistry, client);
    }
}
