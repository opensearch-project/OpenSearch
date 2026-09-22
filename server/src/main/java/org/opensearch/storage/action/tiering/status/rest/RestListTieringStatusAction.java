/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.rest;

import org.opensearch.common.Table;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.rest.action.cat.RestTable;
import org.opensearch.storage.action.tiering.status.ListTieringStatusAction;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for listing tiering status of all indices.
 */
public class RestListTieringStatusAction extends AbstractCatAction {

    private static final String TARGET_TIER = "target";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_tier/all"));
    }

    @Override
    public String getName() {
        return "list_tiering_status";
    }

    @Override
    public RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        String targetTier = request.param(TARGET_TIER);

        targetTier = parseAndValidateTargetTier(targetTier);
        ListTieringStatusRequest listTieringStatusRequest = new ListTieringStatusRequest(targetTier);
        return channel -> client.admin()
            .cluster()
            .execute(
                ListTieringStatusAction.INSTANCE,
                listTieringStatusRequest,
                new RestResponseListener<ListTieringStatusResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ListTieringStatusResponse listTieringStatusResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(request, listTieringStatusResponse), channel);
                    }
                }
            );
    }

    @Override
    public void documentation(StringBuilder stringBuilder) {
        stringBuilder.append("/_tier/all\n");
    }

    @Override
    public Table getTableWithHeader(RestRequest restRequest) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("index", "alias:i,idx;desc:index name");
        table.addCell("state", "desc: the current state of the migration");
        table.addCell("source", "desc: source tier");
        table.addCell("target", "desc: destination tier");
        table.endHeaders();
        return table;
    }

    public Table buildTable(RestRequest request, ListTieringStatusResponse response) {
        Table table = getTableWithHeader(request);
        for (TieringStatus tieringStatus : response.getTieringStatusList()) {
            table.startRow();
            table.addCell(tieringStatus.getIndexName());
            table.addCell(tieringStatus.getStatus());
            table.addCell(tieringStatus.getSource());
            table.addCell(tieringStatus.getTarget());
            table.endRow();
        }
        return table;
    }

    // parse target tier '_hot' and '_warm'
    public String parseAndValidateTargetTier(String targetTier) {
        if (targetTier == null) {
            return null;
        }

        // Extract the part after the underscore
        String[] parts = targetTier.split("_");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid format. Target Tier must be in the form '_tier'");
        }

        // Convert to lowercase for case-insensitive comparison
        String extractedTier = parts[1].toUpperCase(Locale.ROOT);

        // Validate the extracted tier
        if (!TieringUtils.isTerminalTier(extractedTier)) {
            throw new IllegalArgumentException("Invalid target tier. Target Tier must be either '_hot' or '_warm'");
        }

        return extractedTier;

    }
}
