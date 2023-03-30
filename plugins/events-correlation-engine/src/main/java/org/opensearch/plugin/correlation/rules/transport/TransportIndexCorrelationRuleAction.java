/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.rules.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleAction;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleRequest;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportIndexCorrelationRuleAction extends HandledTransportAction<IndexCorrelationRuleRequest, IndexCorrelationRuleResponse> {

    public TransportIndexCorrelationRuleAction(TransportService transportService, Client client, ActionFilters actionFilters) {
        super(IndexCorrelationRuleAction.NAME, transportService, actionFilters, IndexCorrelationRuleRequest::new);
    }

    @Override
    protected void doExecute(Task task, IndexCorrelationRuleRequest request, ActionListener<IndexCorrelationRuleResponse> listener) {

    }
}
