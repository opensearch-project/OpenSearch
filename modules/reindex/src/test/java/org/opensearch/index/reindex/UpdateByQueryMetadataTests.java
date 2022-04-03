/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.reindex;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.index.reindex.ScrollableHitSource.Hit;

public class UpdateByQueryMetadataTests extends AbstractAsyncBulkByScrollActionMetadataTestCase<
    UpdateByQueryRequest,
    BulkByScrollResponse> {

    public void testRoutingIsCopied() {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    @Override
    protected TestAction action() {
        return new TestAction();
    }

    @Override
    protected UpdateByQueryRequest request() {
        return new UpdateByQueryRequest();
    }

    private class TestAction extends TransportUpdateByQueryAction.AsyncIndexBySearchAction {
        TestAction() {
            super(
                UpdateByQueryMetadataTests.this.task,
                UpdateByQueryMetadataTests.this.logger,
                null,
                UpdateByQueryMetadataTests.this.threadPool,
                null,
                request(),
                ClusterState.EMPTY_STATE,
                listener()
            );
        }

        @Override
        public AbstractAsyncBulkByScrollAction.RequestWrapper<?> copyMetadata(
            AbstractAsyncBulkByScrollAction.RequestWrapper<?> request,
            Hit doc
        ) {
            return super.copyMetadata(request, doc);
        }
    }
}
