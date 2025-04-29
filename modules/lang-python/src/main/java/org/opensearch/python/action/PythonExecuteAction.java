/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.python.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.TemplateScript;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

public class PythonExecuteAction extends ActionType<PythonExecuteAction.Response> {
    private static final Logger logger = LogManager.getLogger(PythonExecuteAction.class);

    public static final String NAME = "cluster:admin/scripts/python/execute";
    public static final PythonExecuteAction INSTANCE = new PythonExecuteAction();

    public PythonExecuteAction() {
        super(NAME, Response::new);
    }

    public static class Request extends SingleShardRequest<Request> implements ToXContentObject {
        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "python_execute_request",
            args -> new Request((Script) args[0])
        );
        static final Map<String, ScriptContext<?>> SUPPORTED_CONTEXTS;

        private final Script script;

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
            SUPPORTED_CONTEXTS = Map.of("template", TemplateScript.CONTEXT);
        }

        Request(Script script) {
            Objects.requireNonNull(script);
            if (!Objects.equals(script.getLang(), "python")) {
                logger.warn(
                    "Got a script with language [{}] in a PythoExecuteAction request, ignoring it and setting it to python...",
                    script.getLang()
                );
            }
            this.script = new Script(script.getType(), "python", script.getIdOrCode(), script.getOptions(), script.getParams());
        }

        Request(StreamInput in) throws IOException {
            super(in);
            script = new Script(in);
        }

        static Request parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (script.getType() != ScriptType.INLINE) {
                validationException = addValidationError("only inline scripts are supported", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            script.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Request{" + "script=" + script + "}";

        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Object result;

        Response(Object result) {
            this.result = result;
        }

        public Object getResult() {
            return result;
        }

        /**
         * Write this into the {@linkplain StreamOutput}.
         *
         * @param out
         */
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
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(result, response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(result);
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final ScriptService scriptService;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService,
            ClusterService clusterService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                Request::new,
                ThreadPool.Names.MANAGEMENT
            );
            this.scriptService = scriptService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            return innerShardOperation(request, scriptService);
        }

        static Response innerShardOperation(Request request, ScriptService scriptService) throws IOException {
            // Execute the script using the Python script engine
            TemplateScript.Factory factory = scriptService.compile(request.script, TemplateScript.CONTEXT);
            TemplateScript templateScript = factory.newInstance(Collections.emptyMap());
            String result = templateScript.execute();
            return new Response(result);
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, TransportSingleShardAction<Request, Response>.InternalRequest request) {
            // returns null to execute the operation locally (the node that received the request)
            return null;
        }
    }

    public static class RestAction extends BaseRestHandler {
        @Override
        public List<Route> routes() {
            return List.of(new Route(GET, "/_scripts/python/_execute"), new Route(POST, "/_scripts/python/_execute"));
        }

        @Override
        public String getName() {
            return "_scripts_python_execute";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = Request.parse(restRequest.contentOrSourceParamParser());
            return channel -> client.executeLocally(INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }
}
