/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.opensearch.core.common.Strings.splitStringByCommaToArray;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Base REST handler for tiering operations.
 */
public abstract class RestBaseTierAction extends BaseRestHandler {

    /** The target tier for this action. */
    protected final String targetTier;

    /**
     * Constructs a RestBaseTierAction for the given target tier.
     * @param targetTier the target tier
     */
    protected RestBaseTierAction(String targetTier) {
        this.targetTier = targetTier;
    }

    /** Returns the migration type identifier for this action. @return the migration type */
    protected abstract String getMigrationType();

    /** Returns the action type for this tier operation. @return the tier action type */
    protected abstract ActionType<AcknowledgedResponse> getTierAction();

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/{index}/_tier/" + targetTier));
    }

    /**
     * Validates the indices parameter from the request.
     *
     * @param indices array of index names to validate
     * @throws IllegalArgumentException if validation fails
     */
    protected void validateIndices(final String[] indices) {
        if (CollectionUtils.isEmpty(indices)) {
            throw new IllegalArgumentException("Index parameter is required");
        } else if (indices.length != 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s request should contain exactly one index", getMigrationType())
            );
        } else if (indices[0] == null || indices[0].isBlank()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        } else if (TieringUtils.isMigrationAllowed(indices[0]) == false) {
            throw new IllegalArgumentException("Index is blocklisted for migrations");
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = splitStringByCommaToArray(request.param("index"));

        validateIndices(indices);

        final IndexTieringRequest indexTieringRequest = new IndexTieringRequest(targetTier, indices[0]);
        indexTieringRequest.timeout(request.paramAsTime("timeout", indexTieringRequest.timeout()));
        indexTieringRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", indexTieringRequest.clusterManagerNodeTimeout())
        );
        return channel -> client.admin().cluster().execute(getTierAction(), indexTieringRequest, new RestToXContentListener<>(channel));
    }
}
