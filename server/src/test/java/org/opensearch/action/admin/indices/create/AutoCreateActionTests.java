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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.create;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateDataStreamService;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoCreateActionTests extends OpenSearchTestCase {

    public void testResolveAutoCreateDataStreams() {
        Metadata metadata;
        {
            Metadata.Builder mdBuilder = new Metadata.Builder();
            DataStreamTemplate dataStreamTemplate = new DataStreamTemplate();
            mdBuilder.put("1", new ComposableIndexTemplate(Collections.singletonList("legacy-logs-*"), null, null, 10L, null, null, null));
            mdBuilder.put(
                "2",
                new ComposableIndexTemplate(Collections.singletonList("logs-*"), null, null, 20L, null, null, dataStreamTemplate)
            );
            mdBuilder.put(
                "3",
                new ComposableIndexTemplate(Collections.singletonList("logs-foobar"), null, null, 30L, null, null, dataStreamTemplate)
            );
            metadata = mdBuilder.build();
        }

        CreateIndexRequest request = new CreateIndexRequest("logs-foobar");
        DataStreamTemplate result = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, notNullValue());
        assertThat(result.getTimestampField().getName(), equalTo("@timestamp"));

        request = new CreateIndexRequest("logs-barbaz");
        result = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, notNullValue());
        assertThat(result.getTimestampField().getName(), equalTo("@timestamp"));

        // An index that matches with a template without a data steam definition
        request = new CreateIndexRequest("legacy-logs-foobaz");
        result = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, nullValue());

        // An index that doesn't match with an index template
        request = new CreateIndexRequest("my-index");
        result = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, nullValue());
    }

    public void testResolveIndices() {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        DataStreamTemplate dataStreamTemplate = new DataStreamTemplate();
        mdBuilder.put(
            "1",
            new ComposableIndexTemplate(Collections.singletonList("datastream-*"), null, null, 20L, null, null, dataStreamTemplate)
        );

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        AutoCreateAction.TransportAction action = new AutoCreateAction.TransportAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            mock(MetadataCreateIndexService.class),
            mock(MetadataCreateDataStreamService.class)
        );

        ResolvedIndices resolvedIndices = action.resolveIndices(new CreateIndexRequest("<index-{now/d}>"));
        assertTrue(
            resolvedIndices.toString(),
            resolvedIndices.local().containsAny(resolved -> resolved.matches("index-\\d+\\.\\d+\\.\\d+"))
        );

        resolvedIndices = action.resolveIndices(new CreateIndexRequest("datastream-test"));
        assertEquals(ResolvedIndices.of("datastream-test"), resolvedIndices);
    }

}
