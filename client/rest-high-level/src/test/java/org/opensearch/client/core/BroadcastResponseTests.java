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

package org.opensearch.client.core;

import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.client.AbstractResponseTestCase;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.seqno.RetentionLeaseNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;

public class BroadcastResponseTests extends AbstractResponseTestCase<
    org.opensearch.action.support.broadcast.BroadcastResponse,
    BroadcastResponse> {

    private String index;
    private String id;
    private Set<Integer> shardIds;

    @Override
    protected org.opensearch.action.support.broadcast.BroadcastResponse createServerTestInstance(XContentType xContentType) {
        index = randomAlphaOfLength(8);
        id = randomAlphaOfLength(8);
        final int total = randomIntBetween(1, 16);
        final int successful = total - scaledRandomIntBetween(0, total);
        final int failed = scaledRandomIntBetween(0, total - successful);
        final List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        shardIds = new HashSet<>();
        for (int i = 0; i < failed; i++) {
            final DefaultShardOperationFailedException failure = new DefaultShardOperationFailedException(
                index,
                randomValueOtherThanMany(shardIds::contains, () -> randomIntBetween(0, total - 1)),
                new RetentionLeaseNotFoundException(id)
            );
            failures.add(failure);
            shardIds.add(failure.shardId());
        }

        return new org.opensearch.action.support.broadcast.BroadcastResponse(total, successful, failed, failures);
    }

    @Override
    protected BroadcastResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return BroadcastResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.opensearch.action.support.broadcast.BroadcastResponse serverTestInstance,
        BroadcastResponse clientInstance
    ) {
        assertThat(clientInstance.shards().total(), equalTo(serverTestInstance.getTotalShards()));
        assertThat(clientInstance.shards().successful(), equalTo(serverTestInstance.getSuccessfulShards()));
        assertThat(clientInstance.shards().skipped(), equalTo(0));
        assertThat(clientInstance.shards().failed(), equalTo(serverTestInstance.getFailedShards()));
        assertThat(clientInstance.shards().failures(), hasSize(clientInstance.shards().failed() == 0 ? 0 : 1)); // failures are grouped
        if (clientInstance.shards().failed() > 0) {
            final DefaultShardOperationFailedException groupedFailure = clientInstance.shards().failures().iterator().next();
            assertThat(groupedFailure.index(), equalTo(index));
            assertThat(groupedFailure.shardId(), in(shardIds));
            assertThat(groupedFailure.reason(), containsString("reason=retention lease with ID [" + id + "] not found"));
        }
    }

}
