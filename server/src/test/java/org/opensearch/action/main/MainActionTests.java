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

package org.opensearch.action.main;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MainActionTests extends OpenSearchTestCase {

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("opensearch");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ClusterBlocks blocks;
        if (randomBoolean()) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(
                            randomIntBetween(1, 16),
                            "test global block 400",
                            randomBoolean(),
                            randomBoolean(),
                            false,
                            RestStatus.BAD_REQUEST,
                            ClusterBlockLevel.ALL
                        )
                    )
                    .build();
            }
        } else {
            blocks = ClusterBlocks.builder()
                .addGlobalBlock(
                    new ClusterBlock(
                        randomIntBetween(1, 16),
                        "test global block 503",
                        randomBoolean(),
                        randomBoolean(),
                        false,
                        RestStatus.SERVICE_UNAVAILABLE,
                        ClusterBlockLevel.ALL
                    )
                )
                .build();
        }
        ClusterState state = ClusterState.builder(clusterName).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportMainAction action = new TransportMainAction(settings, transportService, mock(ActionFilters.class), clusterService);
        AtomicReference<MainResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error", e);
            }
        });

        assertNotNull(responseRef.get());
        verify(clusterService, times(1)).state();
    }
}
