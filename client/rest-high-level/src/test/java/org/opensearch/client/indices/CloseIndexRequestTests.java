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

package org.opensearch.client.indices;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.TimedRequest;
import org.opensearch.client.ValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class CloseIndexRequestTests extends OpenSearchTestCase {

    public void testIndices() {
        String[] indices = generateRandomStringArray(5, 5, false, true);
        CloseIndexRequest request = new CloseIndexRequest(indices);
        assertArrayEquals(indices, request.indices());
    }

    public void testValidate() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        CloseIndexRequest request = new CloseIndexRequest(indices);
        Optional<ValidationException> validation = request.validate();
        assertTrue(validation.isPresent());
        assertThat(validation.get().validationErrors().get(0), equalTo("index is missing"));
    }

    public void testIndicesOptions() {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        CloseIndexRequest request = new CloseIndexRequest().indicesOptions(indicesOptions);
        assertEquals(indicesOptions, request.indicesOptions());
    }

    public void testWaitForActiveShards() {
        final CloseIndexRequest request = new CloseIndexRequest("index");
        final int count = randomIntBetween(0, 10);
        request.waitForActiveShards(ActiveShardCount.from(count));
        assertEquals(request.waitForActiveShards(), ActiveShardCount.from(count));
    }

    public void testTimeout() {
        final CloseIndexRequest request = new CloseIndexRequest("index");
        assertEquals(request.timeout(), TimedRequest.DEFAULT_ACK_TIMEOUT);
        assertEquals(request.clusterManagerNodeTimeout(), TimedRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(0, 1000));
        request.setTimeout(timeout);

        final TimeValue clusterManagerTimeout = TimeValue.timeValueSeconds(randomIntBetween(0, 1000));
        request.setClusterManagerTimeout(clusterManagerTimeout);

        assertEquals(request.timeout(), timeout);
        assertEquals(request.clusterManagerNodeTimeout(), clusterManagerTimeout);
    }
}
