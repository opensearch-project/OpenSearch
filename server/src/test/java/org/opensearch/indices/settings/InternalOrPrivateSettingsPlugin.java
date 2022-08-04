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

package org.opensearch.indices.settings;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InternalOrPrivateSettingsPlugin extends Plugin implements ActionPlugin {

    static final Setting<String> INDEX_INTERNAL_SETTING = Setting.simpleString(
        "index.internal",
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    static final Setting<String> INDEX_PRIVATE_SETTING = Setting.simpleString(
        "index.private",
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(INDEX_INTERNAL_SETTING, INDEX_PRIVATE_SETTING);
    }

    public static class UpdateInternalOrPrivateAction extends ActionType<UpdateInternalOrPrivateAction.Response> {

        public static final UpdateInternalOrPrivateAction INSTANCE = new UpdateInternalOrPrivateAction();
        private static final String NAME = "indices:admin/settings/update-internal-or-private-index";

        public UpdateInternalOrPrivateAction() {
            super(NAME, UpdateInternalOrPrivateAction.Response::new);
        }

        public static class Request extends ClusterManagerNodeRequest<Request> {

            private String index;
            private String key;
            private String value;

            Request() {}

            Request(StreamInput in) throws IOException {
                super(in);
                index = in.readString();
                key = in.readString();
                value = in.readString();
            }

            public Request(final String index, final String key, final String value) {
                this.index = index;
                this.key = key;
                this.value = value;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(index);
                out.writeString(key);
                out.writeString(value);
            }

        }

        static class Response extends ActionResponse {
            Response() {}

            Response(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {}
        }

    }

    public static class TransportUpdateInternalOrPrivateAction extends TransportClusterManagerNodeAction<
        UpdateInternalOrPrivateAction.Request,
        UpdateInternalOrPrivateAction.Response> {

        @Inject
        public TransportUpdateInternalOrPrivateAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                UpdateInternalOrPrivateAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UpdateInternalOrPrivateAction.Request::new,
                indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateInternalOrPrivateAction.Response read(StreamInput in) throws IOException {
            return new UpdateInternalOrPrivateAction.Response(in);
        }

        @Override
        protected void clusterManagerOperation(
            final UpdateInternalOrPrivateAction.Request request,
            final ClusterState state,
            final ActionListener<UpdateInternalOrPrivateAction.Response> listener
        ) throws Exception {
            clusterService.submitStateUpdateTask("update-index-internal-or-private", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(final ClusterState currentState) throws Exception {
                    final Metadata.Builder builder = Metadata.builder(currentState.metadata());
                    final IndexMetadata.Builder imdBuilder = IndexMetadata.builder(currentState.metadata().index(request.index));
                    final Settings.Builder settingsBuilder = Settings.builder()
                        .put(currentState.metadata().index(request.index).getSettings())
                        .put(request.key, request.value);
                    imdBuilder.settings(settingsBuilder);
                    imdBuilder.settingsVersion(1 + imdBuilder.settingsVersion());
                    builder.put(imdBuilder.build(), true);
                    return ClusterState.builder(currentState).metadata(builder).build();
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    listener.onResponse(new UpdateInternalOrPrivateAction.Response());
                }

                @Override
                public void onFailure(final String source, final Exception e) {
                    listener.onFailure(e);
                }

            });
        }

        @Override
        protected ClusterBlockException checkBlock(final UpdateInternalOrPrivateAction.Request request, final ClusterState state) {
            return null;
        }

    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
            new ActionHandler<>(UpdateInternalOrPrivateAction.INSTANCE, TransportUpdateInternalOrPrivateAction.class)
        );
    }

}
