/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.querygroup.action.GetQueryGroupRequest;
import org.opensearch.plugin.wlm.querygroup.action.TransportGetQueryGroupAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.Mockito.mock;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.*;

public class TransportGetQueryGroupActionTests extends OpenSearchTestCase {

    /**
     * Test case for ClusterManagerOperation function
     */
    @SuppressWarnings("unchecked")
    public void testClusterManagerOperation() throws Exception {
     }
}
