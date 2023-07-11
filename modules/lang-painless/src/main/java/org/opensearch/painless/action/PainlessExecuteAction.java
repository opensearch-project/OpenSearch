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

package org.opensearch.painless.action;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.script.FilterScript;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

public class PainlessExecuteAction extends ActionType<PainlessExecuteAction.Response> {

    public static final PainlessExecuteAction INSTANCE = new PainlessExecuteAction();
    private static final String NAME = "cluster:admin/scripts/painless/execute";

    private PainlessExecuteAction() {
        super(NAME, Response::new);
    }

    public static class Request extends SingleShardRequest<Request> implements ToXContentObject {

        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ParseField CONTEXT_FIELD = new ParseField("context");
        private static final ParseField CONTEXT_SETUP_FIELD = new ParseField("context_setup");
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "painless_execute_request",
            args -> new Request((Script) args[0], (String) args[1], (ContextSetup) args[2])
        );

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CONTEXT_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ContextSetup::parse, CONTEXT_SETUP_FIELD);
        }

        static final Map<String, ScriptContext<?>> SUPPORTED_CONTEXTS;

        static {
            Map<String, ScriptContext<?>> supportedContexts = new HashMap<>();
            supportedContexts.put("painless_test", PainlessTestScript.CONTEXT);
            supportedContexts.put("filter", FilterScript.CONTEXT);
            supportedContexts.put("score", ScoreScript.CONTEXT);
            SUPPORTED_CONTEXTS = Collections.unmodifiableMap(supportedContexts);
        }

        static ScriptContext<?> fromScriptContextName(String name) {
            ScriptContext<?> scriptContext = SUPPORTED_CONTEXTS.get(name);
            if (scriptContext == null) {
                throw new UnsupportedOperationException("unsupported script context name [" + name + "]");
            }
            return scriptContext;
        }

        static class ContextSetup implements Writeable, ToXContentObject {

            private static final ParseField INDEX_FIELD = new ParseField("index");
            private static final ParseField DOCUMENT_FIELD = new ParseField("document");
            private static final ParseField QUERY_FIELD = new ParseField("query");
            private static final ConstructingObjectParser<ContextSetup, Void> PARSER = new ConstructingObjectParser<>(
                "execute_script_context",
                args -> new ContextSetup((String) args[0], (BytesReference) args[1], (QueryBuilder) args[2])
            );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_FIELD);
                PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                    try (XContentBuilder b = XContentBuilder.builder(p.contentType().xContent())) {
                        b.copyCurrentStructure(p);
                        return BytesReference.bytes(b);
                    }
                }, DOCUMENT_FIELD);
                PARSER.declareObject(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                    QUERY_FIELD
                );
            }

            private final String index;
            private final BytesReference document;
            private final QueryBuilder query;

            private MediaType mediaType;

            static ContextSetup parse(XContentParser parser, Void context) throws IOException {
                ContextSetup contextSetup = PARSER.parse(parser, null);
                contextSetup.setXContentType(parser.contentType());
                return contextSetup;
            }

            ContextSetup(String index, BytesReference document, QueryBuilder query) {
                this.index = index;
                this.document = document;
                this.query = query;
            }

            ContextSetup(StreamInput in) throws IOException {
                index = in.readOptionalString();
                document = in.readOptionalBytesReference();
                String mediaType = in.readOptionalString();
                if (mediaType != null) {
                    this.mediaType = MediaType.fromMediaType(mediaType);
                }
                query = in.readOptionalNamedWriteable(QueryBuilder.class);
            }

            public String getIndex() {
                return index;
            }

            public BytesReference getDocument() {
                return document;
            }

            public QueryBuilder getQuery() {
                return query;
            }

            public MediaType getXContentType() {
                return mediaType;
            }

            public void setXContentType(MediaType mediaType) {
                this.mediaType = mediaType;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ContextSetup that = (ContextSetup) o;
                return Objects.equals(index, that.index)
                    && Objects.equals(document, that.document)
                    && Objects.equals(query, that.query)
                    && Objects.equals(mediaType, that.mediaType);
            }

            @Override
            public int hashCode() {
                return Objects.hash(index, document, query, mediaType);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalString(index);
                out.writeOptionalBytesReference(document);
                out.writeOptionalString(mediaType != null ? mediaType.mediaTypeWithoutParameters() : null);
                out.writeOptionalNamedWriteable(query);
            }

            @Override
            public String toString() {
                return "ContextSetup{"
                    + ", index='"
                    + index
                    + '\''
                    + ", document="
                    + document
                    + ", query="
                    + query
                    + ", xContentType="
                    + mediaType
                    + '}';
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                {
                    if (index != null) {
                        builder.field(INDEX_FIELD.getPreferredName(), index);
                    }
                    if (document != null) {
                        builder.field(DOCUMENT_FIELD.getPreferredName());
                        try (
                            XContentParser parser = XContentHelper.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                document,
                                mediaType
                            )
                        ) {
                            builder.generator().copyCurrentStructure(parser);
                        }
                    }
                    if (query != null) {
                        builder.field(QUERY_FIELD.getPreferredName(), query);
                    }
                }
                builder.endObject();
                return builder;
            }

        }

        private final Script script;
        private final ScriptContext<?> context;
        private final ContextSetup contextSetup;

        static Request parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        Request(Script script, String scriptContextName, ContextSetup setup) {
            this.script = Objects.requireNonNull(script);
            this.context = scriptContextName != null ? fromScriptContextName(scriptContextName) : PainlessTestScript.CONTEXT;
            if (setup != null) {
                this.contextSetup = setup;
                index(contextSetup.index);
            } else {
                contextSetup = null;
            }
        }

        Request(StreamInput in) throws IOException {
            super(in);
            script = new Script(in);
            context = fromScriptContextName(in.readString());
            contextSetup = in.readOptionalWriteable(ContextSetup::new);
        }

        public Script getScript() {
            return script;
        }

        public ScriptContext<?> getContext() {
            return context;
        }

        public ContextSetup getContextSetup() {
            return contextSetup;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (script.getType() != ScriptType.INLINE) {
                validationException = addValidationError("only inline scripts are supported", validationException);
            }
            if (needDocumentAndIndex(context)) {
                if (contextSetup.index == null) {
                    validationException = addValidationError("index is a required parameter for current context", validationException);
                }
                if (contextSetup.document == null) {
                    validationException = addValidationError("document is a required parameter for current context", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            script.writeTo(out);
            out.writeString(context.name);
            out.writeOptionalWriteable(contextSetup);
        }

        // For testing only:
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.field(CONTEXT_FIELD.getPreferredName(), context.name);
            if (contextSetup != null) {
                builder.field(CONTEXT_SETUP_FIELD.getPreferredName(), contextSetup);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(script, request.script)
                && Objects.equals(context, request.context)
                && Objects.equals(contextSetup, request.contextSetup);
        }

        @Override
        public int hashCode() {
            return Objects.hash(script, context, contextSetup);
        }

        @Override
        public String toString() {
            return "Request{" + "script=" + script + "context=" + context + ", contextSetup=" + contextSetup + '}';
        }

        static boolean needDocumentAndIndex(ScriptContext<?> scriptContext) {
            return scriptContext == FilterScript.CONTEXT || scriptContext == ScoreScript.CONTEXT;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Object result;

        Response(Object result) {
            this.result = result;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            result = in.readGenericValue();
        }

        public Object getResult() {
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", result);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(result, response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }

    public abstract static class PainlessTestScript {

        private final Map<String, Object> params;

        public PainlessTestScript(Map<String, Object> params) {
            this.params = params;
        }

        /** Return the parameters for this script. */
        public Map<String, Object> getParams() {
            return params;
        }

        public abstract Object execute();

        public interface Factory {

            PainlessTestScript newInstance(Map<String, Object> params);

        }

        public static final String[] PARAMETERS = {};
        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("painless_test", Factory.class);

    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final ScriptService scriptService;
        private final IndicesService indicesServices;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService,
            ClusterService clusterService,
            IndicesService indicesServices
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                // Forking a thread here, because only light weight operations should happen on network thread and
                // Creating a in-memory index is not light weight
                // TODO: is MANAGEMENT TP the right TP? Right now this is an admin api (see action name).
                Request::new,
                ThreadPool.Names.MANAGEMENT
            );
            this.scriptService = scriptService;
            this.indicesServices = indicesServices;
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
            if (request.concreteIndex() != null) {
                return super.checkRequestBlock(state, request);
            }
            return null;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return request.contextSetup != null && request.contextSetup.getIndex() != null;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            if (request.concreteIndex() == null) {
                return null;
            }
            return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexService indexService;
            if (request.contextSetup != null && request.contextSetup.getIndex() != null) {
                ClusterState clusterState = clusterService.state();
                IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();
                String indexExpression = request.contextSetup.index;
                Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions, indexExpression);
                if (concreteIndices.length != 1) {
                    throw new IllegalArgumentException("[" + indexExpression + "] does not resolve to a single index");
                }
                Index concreteIndex = concreteIndices[0];
                indexService = indicesServices.indexServiceSafe(concreteIndex);
            } else {
                indexService = null;
            }
            return innerShardOperation(request, scriptService, indexService);
        }

        static Response innerShardOperation(Request request, ScriptService scriptService, IndexService indexService) throws IOException {
            final ScriptContext<?> scriptContext = request.context;
            if (scriptContext == PainlessTestScript.CONTEXT) {
                PainlessTestScript.Factory factory = scriptService.compile(request.script, PainlessTestScript.CONTEXT);
                PainlessTestScript painlessTestScript = factory.newInstance(request.script.getParams());
                String result = Objects.toString(painlessTestScript.execute());
                return new Response(result);
            } else if (scriptContext == FilterScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    FilterScript.Factory factory = scriptService.compile(request.script, FilterScript.CONTEXT);
                    FilterScript.LeafFactory leafFactory = factory.newFactory(request.getScript().getParams(), context.lookup());
                    FilterScript filterScript = leafFactory.newInstance(leafReaderContext);
                    filterScript.setDocument(0);
                    boolean result = filterScript.execute();
                    return new Response(result);
                }, indexService);
            } else if (scriptContext == ScoreScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    ScoreScript.Factory factory = scriptService.compile(request.script, ScoreScript.CONTEXT);
                    ScoreScript.LeafFactory leafFactory = factory.newFactory(request.getScript().getParams(), context.lookup());
                    ScoreScript scoreScript = leafFactory.newInstance(leafReaderContext);
                    scoreScript.setDocument(0);

                    if (request.contextSetup.query != null) {
                        Query luceneQuery = request.contextSetup.query.rewrite(context).toQuery(context);
                        IndexSearcher indexSearcher = new IndexSearcher(leafReaderContext.reader());
                        luceneQuery = indexSearcher.rewrite(luceneQuery);
                        Weight weight = indexSearcher.createWeight(luceneQuery, ScoreMode.COMPLETE, 1f);
                        Scorer scorer = weight.scorer(indexSearcher.getIndexReader().leaves().get(0));
                        // Consume the first (and only) match.
                        int docID = scorer.iterator().nextDoc();
                        assert docID == scorer.docID();
                        scoreScript.setScorer(scorer);
                    }

                    double result = scoreScript.execute(null);
                    return new Response(result);
                }, indexService);
            } else {
                throw new UnsupportedOperationException("unsupported context [" + scriptContext.name + "]");
            }
        }

        private static Response prepareRamIndex(
            Request request,
            CheckedBiFunction<QueryShardContext, LeafReaderContext, Response, IOException> handler,
            IndexService indexService
        ) throws IOException {

            Analyzer defaultAnalyzer = indexService.getIndexAnalyzers().getDefaultIndexAnalyzer();

            try (Directory directory = new ByteBuffersDirectory()) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(defaultAnalyzer))) {
                    String index = indexService.index().getName();
                    BytesReference document = request.contextSetup.document;
                    MediaType mediaType = request.contextSetup.mediaType;
                    SourceToParse sourceToParse = new SourceToParse(index, "_id", document, mediaType);
                    ParsedDocument parsedDocument = indexService.mapperService().documentMapper().parse(sourceToParse);
                    indexWriter.addDocuments(parsedDocument.docs());
                    try (IndexReader indexReader = DirectoryReader.open(indexWriter)) {
                        final IndexSearcher searcher = new IndexSearcher(indexReader);
                        searcher.setQueryCache(null);
                        final long absoluteStartMillis = System.currentTimeMillis();
                        QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> absoluteStartMillis, null);
                        return handler.apply(context, indexReader.leaves().get(0));
                    }
                }
            }
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(new Route(GET, "/_scripts/painless/_execute"), new Route(POST, "/_scripts/painless/_execute")));
        }

        @Override
        public String getName() {
            return "_scripts_painless_execute";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = Request.parse(restRequest.contentOrSourceParamParser());
            return channel -> client.executeLocally(INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }

}
