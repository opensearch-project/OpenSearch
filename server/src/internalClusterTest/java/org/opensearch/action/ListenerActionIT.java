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

package org.opensearch.action;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ListenerActionIT extends OpenSearchIntegTestCase {
    public void testThreadedListeners() throws Throwable {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<String> threadName = new AtomicReference<>();
        Client client = client();

        IndexRequest request = new IndexRequest("test").id("1");
        if (randomBoolean()) {
            // set the source, without it, we will have a verification failure
            request.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
        }

        client.index(request, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                threadName.set(Thread.currentThread().getName());
                failure.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertFalse(threadName.get().contains("listener"));
    }
}
