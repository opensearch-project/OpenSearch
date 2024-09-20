/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.rest.pagination.PageParams;

import java.io.IOException;
import java.util.Objects;

/**
 * Base Transport action class for _list API.
 *
 * @opensearch.api
 */
public abstract class AbstractListAction extends AbstractCatAction {

    protected PageParams pageParams;

    protected abstract void documentation(StringBuilder sb);

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        boolean helpWanted = request.paramAsBoolean("help", false);
        if (helpWanted || isActionPaginated() == false) {
            return super.prepareRequest(request, client);
        }
        this.pageParams = validateAndGetPageParams(request);
        assert Objects.nonNull(pageParams) : "pageParams can not be null for paginated queries";
        return doCatRequest(request, client);
    }

    /**
     *
     * @return boolean denoting whether the RestAction will output paginated responses or not.
     * Is kept false by default, every paginated action to override and return true.
     */
    public boolean isActionPaginated() {
        return false;
    }

    /**
     *
     * @return Metadata that can be extracted out from the rest request. Each paginated action to override and provide
     * its own implementation. Query params supported by the action specific to pagination along with the respective validations,
     * should be added here.
     */
    protected PageParams validateAndGetPageParams(RestRequest restRequest) {
        return null;
    }

}
