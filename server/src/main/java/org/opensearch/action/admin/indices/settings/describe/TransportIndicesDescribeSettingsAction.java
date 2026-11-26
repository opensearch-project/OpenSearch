/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.settings.describe;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SettingsDescriber;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Resolves index setting definitions from the node's {@link IndexScopedSettings} registry. The registry is identical on
 * every node of the same version and is shared by every index on the node ({@code IndexScopedSettings#copy} reuses the
 * definition maps), so the request is answered locally by whichever node receives it, exactly as the cluster describe
 * action does: no cluster-manager round-trip, no cluster blocks consulted, and the response never crosses the wire.
 * Authorization is unaffected; it is applied by {@code ActionFilter}s before {@link #doExecute} runs. With no index in the
 * request, an index-level privilege for {@code indices:monitor/settings/describe} is evaluated against all indices, as it
 * is for {@code GET /_settings} without an index.
 * @opensearch.internal
 */
public class TransportIndicesDescribeSettingsAction extends HandledTransportAction<
    IndicesDescribeSettingsRequest,
    IndicesDescribeSettingsResponse> {
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportIndicesDescribeSettingsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter
    ) {
        super(IndicesDescribeSettingsAction.NAME, transportService, actionFilters, IndicesDescribeSettingsRequest::new);
        this.indexScopedSettings = indexScopedSettings;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected void doExecute(Task task, IndicesDescribeSettingsRequest request, ActionListener<IndicesDescribeSettingsResponse> listener) {
        listener.onResponse(
            new IndicesDescribeSettingsResponse(SettingsDescriber.describe(request.names(), indexScopedSettings, settingsFilter))
        );
    }
}
