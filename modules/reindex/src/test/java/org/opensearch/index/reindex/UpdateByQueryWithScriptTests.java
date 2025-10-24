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

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.script.ScriptService;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class UpdateByQueryWithScriptTests extends AbstractAsyncBulkByScrollActionScriptTestCase<
    UpdateByQueryRequest,
    BulkByScrollResponse> {

    public void testModifyingCtxNotAllowed() {
        /*
         * Its important that none of these actually match any of the fields.
         * They don't now, but make sure they still don't match if you add any
         * more. The point of have many is that they should all present the same
         * error message to the user, not some ClassCastException.
         */
        Object[] options = new Object[] { "cat", new Object(), 123, new Date(), Math.PI };
        for (String ctxVar : new String[] { "_index", "_id", "_version", "_routing" }) {
            try {
                applyScript((Map<String, Object> ctx) -> ctx.put(ctxVar, randomFrom(options)));
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("Modifying [" + ctxVar + "] not allowed"));
            }
        }
    }

    @Override
    protected UpdateByQueryRequest request() {
        return new UpdateByQueryRequest();
    }

    @Override
    protected TransportUpdateByQueryAction.AsyncIndexBySearchAction action(ScriptService scriptService, UpdateByQueryRequest request) {
        TransportService transportService = mock(TransportService.class);
        TransportUpdateByQueryAction transportAction = new TransportUpdateByQueryAction(
            threadPool,
            new ActionFilters(Collections.emptySet()),
            null,
            transportService,
            scriptService,
            null,
            null
        );
        return new TransportUpdateByQueryAction.AsyncIndexBySearchAction(
            task,
            logger,
            null,
            threadPool,
            scriptService,
            request,
            ClusterState.EMPTY_STATE,
            listener()
        );
    }
}
