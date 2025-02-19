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

package org.opensearch.action.admin.indices.resolve;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexAbstractionResolver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

/**
 * Transport action to resolve an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ResolveIndexAction extends ActionType<ResolveIndexAction.Response> {

    public static final ResolveIndexAction INSTANCE = new ResolveIndexAction();
    public static final String NAME = "indices:admin/resolve/index";

    private ResolveIndexAction() {
        super(NAME, Response::new);
    }

    /**
     * Request for resolving an index
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

        private String[] names;
        private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

        public Request(String[] names) {
            this.names = names;
        }

        public Request(String[] names, IndicesOptions indicesOptions) {
            this.names = names;
            this.indicesOptions = indicesOptions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }
    }

    /**
     * Abstraction class for resolving an index
     *
     * @opensearch.internal
     */
    public static class ResolvedIndexAbstraction {

        static final ParseField NAME_FIELD = new ParseField("name");

        private String name;

        ResolvedIndexAbstraction() {}

        ResolvedIndexAbstraction(String name) {
            this.name = name;
        }

        protected void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * The resolved index
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ResolvedIndex extends ResolvedIndexAbstraction implements Writeable, ToXContentObject {

        static final ParseField ALIASES_FIELD = new ParseField("aliases");
        static final ParseField ATTRIBUTES_FIELD = new ParseField("attributes");
        static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");

        private final String[] aliases;
        private final String[] attributes;
        private final String dataStream;

        ResolvedIndex(StreamInput in) throws IOException {
            setName(in.readString());
            this.aliases = in.readStringArray();
            this.attributes = in.readStringArray();
            this.dataStream = in.readOptionalString();
        }

        ResolvedIndex(String name, String[] aliases, String[] attributes, @Nullable String dataStream) {
            super(name);
            this.aliases = aliases;
            this.attributes = attributes;
            this.dataStream = dataStream;
        }

        public ResolvedIndex copy(String newName) {
            return new ResolvedIndex(newName, aliases, attributes, dataStream);
        }

        public String[] getAliases() {
            return aliases;
        }

        public String[] getAttributes() {
            return attributes;
        }

        public String getDataStream() {
            return dataStream;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(aliases);
            out.writeStringArray(attributes);
            out.writeOptionalString(dataStream);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), getName());
            if (aliases.length > 0) {
                builder.array(ALIASES_FIELD.getPreferredName(), aliases);
            }
            builder.array(ATTRIBUTES_FIELD.getPreferredName(), attributes);
            if (Strings.isNullOrEmpty(dataStream) == false) {
                builder.field(DATA_STREAM_FIELD.getPreferredName(), dataStream);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedIndex index = (ResolvedIndex) o;
            return getName().equals(index.getName())
                && Objects.equals(dataStream, index.dataStream)
                && Arrays.equals(aliases, index.aliases)
                && Arrays.equals(attributes, index.attributes);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName(), dataStream);
            result = 31 * result + Arrays.hashCode(aliases);
            result = 31 * result + Arrays.hashCode(attributes);
            return result;
        }
    }

    /**
     * The resolved index alias
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ResolvedAlias extends ResolvedIndexAbstraction implements Writeable, ToXContentObject {

        static final ParseField INDICES_FIELD = new ParseField("indices");

        private final String[] indices;

        ResolvedAlias(StreamInput in) throws IOException {
            setName(in.readString());
            this.indices = in.readStringArray();
        }

        ResolvedAlias(String name, String[] indices) {
            super(name);
            this.indices = indices;
        }

        public ResolvedAlias copy(String newName) {
            return new ResolvedAlias(newName, indices);
        }

        public String[] getIndices() {
            return indices;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(indices);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), getName());
            if (indices.length > 0) {
                builder.array(INDICES_FIELD.getPreferredName(), indices);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedAlias alias = (ResolvedAlias) o;
            return getName().equals(alias.getName()) && Arrays.equals(indices, alias.indices);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName());
            result = 31 * result + Arrays.hashCode(indices);
            return result;
        }
    }

    /**
     * The resolved data stream
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ResolvedDataStream extends ResolvedIndexAbstraction implements Writeable, ToXContentObject {

        static final ParseField BACKING_INDICES_FIELD = new ParseField("backing_indices");
        static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp_field");

        private final String[] backingIndices;
        private final String timestampField;

        ResolvedDataStream(StreamInput in) throws IOException {
            setName(in.readString());
            this.backingIndices = in.readStringArray();
            this.timestampField = in.readString();
        }

        ResolvedDataStream(String name, String[] backingIndices, String timestampField) {
            super(name);
            this.backingIndices = backingIndices;
            this.timestampField = timestampField;
        }

        public ResolvedDataStream copy(String newName) {
            return new ResolvedDataStream(newName, backingIndices, timestampField);
        }

        public String[] getBackingIndices() {
            return backingIndices;
        }

        public String getTimestampField() {
            return timestampField;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(backingIndices);
            out.writeString(timestampField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), getName());
            builder.array(BACKING_INDICES_FIELD.getPreferredName(), backingIndices);
            builder.field(TIMESTAMP_FIELD.getPreferredName(), timestampField);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedDataStream dataStream = (ResolvedDataStream) o;
            return getName().equals(dataStream.getName())
                && timestampField.equals(dataStream.timestampField)
                && Arrays.equals(backingIndices, dataStream.backingIndices);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName(), timestampField);
            result = 31 * result + Arrays.hashCode(backingIndices);
            return result;
        }
    }

    /**
     * Response for resolving an index
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Response extends ActionResponse implements ToXContentObject {

        static final ParseField INDICES_FIELD = new ParseField("indices");
        static final ParseField ALIASES_FIELD = new ParseField("aliases");
        static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");

        private final List<ResolvedIndex> indices;
        private final List<ResolvedAlias> aliases;
        private final List<ResolvedDataStream> dataStreams;

        public Response(List<ResolvedIndex> indices, List<ResolvedAlias> aliases, List<ResolvedDataStream> dataStreams) {
            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = dataStreams;
        }

        public Response(StreamInput in) throws IOException {
            this.indices = in.readList(ResolvedIndex::new);
            this.aliases = in.readList(ResolvedAlias::new);
            this.dataStreams = in.readList(ResolvedDataStream::new);
        }

        public List<ResolvedIndex> getIndices() {
            return indices;
        }

        public List<ResolvedAlias> getAliases() {
            return aliases;
        }

        public List<ResolvedDataStream> getDataStreams() {
            return dataStreams;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(indices);
            out.writeList(aliases);
            out.writeList(dataStreams);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDICES_FIELD.getPreferredName(), indices);
            builder.field(ALIASES_FIELD.getPreferredName(), aliases);
            builder.field(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return indices.equals(response.indices) && aliases.equals(response.aliases) && dataStreams.equals(response.dataStreams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, aliases, dataStreams);
        }
    }

    /**
     * Transport action for resolving an index
     *
     * @opensearch.internal
     */
    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ThreadPool threadPool;
        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final IndexAbstractionResolver indexAbstractionResolver;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(NAME, transportService, actionFilters, Request::new);
            this.threadPool = threadPool;
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        }

        @Override
        protected void doExecute(Task task, Request request, final ActionListener<Response> listener) {
            final ClusterState clusterState = clusterService.state();
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
                request.indicesOptions(),
                request.indices(),
                idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterState)
            );
            final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            final Metadata metadata = clusterState.metadata();
            List<ResolvedIndex> indices = new ArrayList<>();
            List<ResolvedAlias> aliases = new ArrayList<>();
            List<ResolvedDataStream> dataStreams = new ArrayList<>();
            if (localIndices != null) {
                resolveIndices(
                    localIndices.indices(),
                    request.indicesOptions,
                    metadata,
                    indexAbstractionResolver,
                    indices,
                    aliases,
                    dataStreams,
                    request.includeDataStreams()
                );
            }

            if (remoteClusterIndices.size() > 0) {
                final int remoteRequests = remoteClusterIndices.size();
                final CountDown completionCounter = new CountDown(remoteRequests);
                final SortedMap<String, Response> remoteResponses = Collections.synchronizedSortedMap(new TreeMap<>());
                final Runnable terminalHandler = () -> {
                    if (completionCounter.countDown()) {
                        mergeResults(remoteResponses, indices, aliases, dataStreams);
                        listener.onResponse(new Response(indices, aliases, dataStreams));
                    }
                };

                // make the cross-cluster calls
                for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                    String clusterAlias = remoteIndices.getKey();
                    OriginalIndices originalIndices = remoteIndices.getValue();
                    Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
                    Request remoteRequest = new Request(originalIndices.indices(), originalIndices.indicesOptions());
                    remoteClusterClient.admin().indices().resolveIndex(remoteRequest, ActionListener.wrap(response -> {
                        remoteResponses.put(clusterAlias, response);
                        terminalHandler.run();
                    }, failure -> terminalHandler.run()));
                }
            } else {
                listener.onResponse(new Response(indices, aliases, dataStreams));
            }
        }

        /**
         * Resolves the specified names and/or wildcard expressions to index abstractions. Returns results in the supplied lists.
         *
         * @param names          The names and wildcard expressions to resolve
         * @param indicesOptions Options for expanding wildcards to indices with different states
         * @param metadata       Cluster metadata
         * @param resolver       Resolver instance for matching names
         * @param indices        List containing any matching indices
         * @param aliases        List containing any matching aliases
         * @param dataStreams    List containing any matching data streams
         */
        // visible for testing
        static void resolveIndices(
            String[] names,
            IndicesOptions indicesOptions,
            Metadata metadata,
            IndexAbstractionResolver resolver,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            boolean includeDataStreams
        ) {
            List<String> resolvedIndexAbstractions = resolver.resolveIndexAbstractions(names, indicesOptions, metadata, includeDataStreams);
            SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
            for (String s : resolvedIndexAbstractions) {
                enrichIndexAbstraction(s, lookup, indices, aliases, dataStreams);
            }
            indices.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            aliases.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            dataStreams.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));

        }

        private static void mergeResults(
            Map<String, Response> remoteResponses,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams
        ) {
            for (Map.Entry<String, Response> responseEntry : remoteResponses.entrySet()) {
                String clusterAlias = responseEntry.getKey();
                Response response = responseEntry.getValue();
                for (ResolvedIndex index : response.indices) {
                    indices.add(index.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index.getName())));
                }
                for (ResolvedAlias alias : response.aliases) {
                    aliases.add(alias.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, alias.getName())));
                }
                for (ResolvedDataStream dataStream : response.dataStreams) {
                    dataStreams.add(dataStream.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, dataStream.getName())));
                }
            }
        }

        private static void enrichIndexAbstraction(
            String indexAbstraction,
            SortedMap<String, IndexAbstraction> lookup,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams
        ) {
            IndexAbstraction ia = lookup.get(indexAbstraction);
            if (ia != null) {
                switch (ia.getType()) {
                    case CONCRETE_INDEX:
                        IndexAbstraction.Index index = (IndexAbstraction.Index) ia;

                        String[] aliasNames = StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(index.getWriteIndex().getAliases().keySet().iterator(), 0),
                            false
                        ).toArray(String[]::new);
                        Arrays.sort(aliasNames);

                        List<String> attributes = new ArrayList<>();
                        attributes.add(index.getWriteIndex().getState() == IndexMetadata.State.OPEN ? "open" : "closed");
                        if (ia.isHidden()) {
                            attributes.add("hidden");
                        }
                        final boolean isFrozen = Boolean.parseBoolean(ia.getWriteIndex().getSettings().get("index.frozen"));
                        if (isFrozen) {
                            attributes.add("frozen");
                        }
                        attributes.sort(String::compareTo);

                        indices.add(
                            new ResolvedIndex(
                                index.getName(),
                                aliasNames,
                                attributes.toArray(Strings.EMPTY_ARRAY),
                                index.getParentDataStream() == null ? null : index.getParentDataStream().getName()
                            )
                        );
                        break;
                    case ALIAS:
                        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) ia;
                        String[] indexNames = alias.getIndices().stream().map(i -> i.getIndex().getName()).toArray(String[]::new);
                        Arrays.sort(indexNames);
                        aliases.add(new ResolvedAlias(alias.getName(), indexNames));
                        break;
                    case DATA_STREAM:
                        IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) ia;
                        String[] backingIndices = dataStream.getIndices().stream().map(i -> i.getIndex().getName()).toArray(String[]::new);
                        Arrays.sort(backingIndices);
                        dataStreams.add(
                            new ResolvedDataStream(
                                dataStream.getName(),
                                backingIndices,
                                dataStream.getDataStream().getTimeStampField().getName()
                            )
                        );
                        break;
                    default:
                        throw new IllegalStateException("unknown index abstraction type: " + ia.getType());
                }
            }
        }
    }
}
