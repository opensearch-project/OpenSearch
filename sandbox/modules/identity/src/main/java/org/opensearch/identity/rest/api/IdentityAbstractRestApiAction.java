/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.DefaultObjectMapper;
import org.opensearch.identity.User;
import org.opensearch.identity.rest.action.ConfigUpdateAction;
import org.opensearch.identity.rest.request.ConfigUpdateRequest;
import org.opensearch.identity.rest.response.ConfigUpdateResponse;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.ConfigurationRepository;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.rest.validation.AbstractConfigurationValidator;
import org.opensearch.identity.rest.validation.AbstractConfigurationValidator.ErrorType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

public abstract class IdentityAbstractRestApiAction extends BaseRestHandler {

    protected final Logger log = LogManager.getLogger(this.getClass());

    protected final ConfigurationRepository cl;
    protected final ClusterService cs;
    final ThreadPool threadPool;
    protected String identityIndex;
    private final RestApiPrivilegesEvaluator restApiPrivilegesEvaluator;
    protected final Settings settings;

    protected IdentityAbstractRestApiAction(
        final Settings settings,
        final Path configPath,
        final RestController controller,
        final Client client,
        final ConfigurationRepository cl,
        final ClusterService cs,
        ThreadPool threadPool
    ) {

        super();
        this.settings = settings;
        this.identityIndex = settings.get(ConfigConstants.IDENTITY_CONFIG_INDEX_NAME, ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);

        this.restApiPrivilegesEvaluator = new RestApiPrivilegesEvaluator();

        this.cl = cl;
        this.cs = cs;
        this.threadPool = threadPool;
    }

    protected abstract AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... params);

    protected abstract String getResourceName();

    protected abstract CType getConfigName();

    protected void handleApiRequest(final RestChannel channel, final RestRequest request, final Client client) throws IOException {

        try {
            // validate additional settings, if any
            AbstractConfigurationValidator validator = getValidator(request, request.content());
            if (!validator.validate()) {
                request.params().clear();
                badRequestResponse(channel, validator);
                return;
            }
            switch (request.method()) {
                case DELETE:
                    handleDelete(channel, request, client, validator.getContentAsNode());
                    break;
                case POST:
                    handlePost(channel, request, client, validator.getContentAsNode());
                    break;
                case PUT:
                    handlePut(channel, request, client, validator.getContentAsNode());
                    break;
                case GET:
                    handleGet(channel, request, client, validator.getContentAsNode());
                    break;
                default:
                    throw new IllegalArgumentException(request.method() + " not supported");
            }
        } catch (JsonMappingException jme) {
            throw jme;
            // TODO strip source
            // if(jme.getLocation() == null || jme.getLocation().getSourceRef() == null) {
            // throw jme;
            // } else throw new JsonMappingException(null, jme.getMessage());
        }
    }

    protected void handleDelete(final RestChannel channel, final RestRequest request, final Client client, final JsonNode content)
        throws IOException {
        final String name = request.param("username");

        if (name == null || name.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        final SecurityDynamicConfiguration<?> existingConfiguration = load(getConfigName());

        boolean existed = existingConfiguration.exists(name);
        existingConfiguration.remove(name);

        if (existed) {
            saveAndUpdateConfigs(
                client,
                request,
                getConfigName(),
                existingConfiguration,
                new OnSucessActionListener<IndexResponse>(channel) {

                    @Override
                    public void onResponse(IndexResponse response) {
                        successResponse(channel, "'" + name + "' deleted.");
                    }
                }
            );

        } else {
            notFound(channel, getResourceName() + " " + name + " not found.");
        }
    }

    protected void handlePut(final RestChannel channel, final RestRequest request, final Client client, final JsonNode content)
        throws IOException {

        final String name = request.param("name");

        if (name == null || name.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        final SecurityDynamicConfiguration<?> existingConfiguration = load(getConfigName());

        if (log.isTraceEnabled() && content != null) {
            log.trace(content.toString());
        }

        boolean existed = existingConfiguration.exists(name);
        existingConfiguration.putCObject(name, DefaultObjectMapper.readTree(content, existingConfiguration.getImplementingClass()));

        saveAndUpdateConfigs(client, request, getConfigName(), existingConfiguration, new OnSucessActionListener<IndexResponse>(channel) {

            @Override
            public void onResponse(IndexResponse response) {
                if (existed) {
                    successResponse(channel, "'" + name + "' updated.");
                } else {
                    createdResponse(channel, "'" + name + "' created.");
                }

            }
        });

    }

    protected void handlePost(final RestChannel channel, final RestRequest request, final Client client, final JsonNode content)
        throws IOException {
        notImplemented(channel, Method.POST);
    }

    protected void handleGet(final RestChannel channel, RestRequest request, Client client, final JsonNode content) throws IOException {

        final String resourcename = request.param("name");

        final SecurityDynamicConfiguration<?> configuration = load(getConfigName());

        // no specific resource requested, return complete config
        if (resourcename == null || resourcename.length() == 0) {

            successResponse(channel, configuration);
            return;
        }

        if (!configuration.exists(resourcename)) {
            notFound(channel, "Resource '" + resourcename + "' not found.");
            return;
        }

        configuration.removeOthers(resourcename);
        successResponse(channel, configuration);

        return;
    }

    protected final SecurityDynamicConfiguration<?> load(final CType config) {
        SecurityDynamicConfiguration<?> loaded = cl.getConfigurationsFromIndex(Collections.singleton(config)).get(config).deepClone();
        return loaded;
    }

    protected boolean ensureIndexExists() {
        if (!cs.state().metadata().hasConcreteIndex(this.identityIndex)) {
            return false;
        }
        return true;
    }

    abstract class OnSucessActionListener<Response> implements ActionListener<Response> {

        private final RestChannel channel;

        public OnSucessActionListener(RestChannel channel) {
            super();
            this.channel = channel;
        }

        @Override
        public final void onFailure(Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                conflict(channel, e.getMessage());
            } else {
                internalErrorResponse(channel, "Error " + e.getMessage());
            }
        }

    }

    protected void saveAndUpdateConfigs(
        final Client client,
        final RestRequest request,
        final CType cType,
        final SecurityDynamicConfiguration<?> configuration,
        OnSucessActionListener<IndexResponse> actionListener
    ) {
        final IndexRequest ir = new IndexRequest(this.identityIndex);

        final String id = cType.toLCString();

        try {
            client.index(
                ir.id(id)
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .setIfSeqNo(configuration.getSeqNo())
                    .setIfPrimaryTerm(configuration.getPrimaryTerm())
                    .source(id, XContentHelper.toXContent(configuration, XContentType.JSON, false)),
                new ConfigUpdatingActionListener<>(new String[] { id }, client, actionListener)
            );
        } catch (IOException e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    protected static class ConfigUpdatingActionListener<Response> implements ActionListener<Response> {
        private final String[] cTypes;
        private final Client client;
        private final ActionListener<Response> delegate;

        public ConfigUpdatingActionListener(String[] cTypes, Client client, ActionListener<Response> delegate) {
            this.cTypes = Objects.requireNonNull(cTypes, "cTypes must not be null");
            this.client = Objects.requireNonNull(client, "client must not be null");
            this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        }

        @Override
        public void onResponse(Response response) {

            final ConfigUpdateRequest cur = new ConfigUpdateRequest(cTypes);

            client.execute(ConfigUpdateAction.INSTANCE, cur, new ActionListener<>() {
                @Override
                public void onResponse(final ConfigUpdateResponse ur) {
                    if (ur.hasFailures()) {
                        delegate.onFailure(ur.failures().get(0));
                        return;
                    }
                    delegate.onResponse(response);
                }

                @Override
                public void onFailure(final Exception e) {
                    delegate.onFailure(e);
                }
            });

        }

        @Override
        public void onFailure(Exception e) {
            delegate.onFailure(e);
        }

    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // consume all parameters first so we can return a correct HTTP status,
        // not 400
        consumeParameters(request);

        // check if .opendistro_security index has been initialized
        if (!ensureIndexExists()) {
            return channel -> internalErrorResponse(channel, ErrorType.IDENTITY_NOT_INITIALIZED.getMessage());
        }

        // check if request is authorized
        String authError = restApiPrivilegesEvaluator.checkAccessPermissions(request, getEndpoint());

        final User user = (User) threadPool.getThreadContext().getTransient(ConfigConstants.IDENTITY_USER);
        final String userName = user == null ? null : user.getUsername().getName();
        if (authError != null) {
            log.error("No permission to access REST API: " + authError);
            // for rest request
            request.params().clear();
            return channel -> forbidden(channel, "No permission to access REST API: " + authError);
        }

        final Object originalUser = threadPool.getThreadContext().getTransient(ConfigConstants.IDENTITY_USER);
        final Object originalRemoteAddress = threadPool.getThreadContext().getTransient(ConfigConstants.IDENTITY_REMOTE_ADDRESS);
        final Object originalOrigin = threadPool.getThreadContext().getTransient(ConfigConstants.IDENTITY_ORIGIN);

        return channel -> threadPool.generic().submit(() -> {
            try (StoredContext ignore = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(ConfigConstants.IDENTITY_CONF_REQUEST_HEADER, "true");
                threadPool.getThreadContext().putTransient(ConfigConstants.IDENTITY_USER, originalUser);
                threadPool.getThreadContext().putTransient(ConfigConstants.IDENTITY_REMOTE_ADDRESS, originalRemoteAddress);
                threadPool.getThreadContext().putTransient(ConfigConstants.IDENTITY_ORIGIN, originalOrigin);

                handleApiRequest(channel, request, client);
            } catch (Exception e) {
                log.error("Error processing request {}", request, e);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException ioe) {
                    throw ExceptionsHelper.convertToOpenSearchException(e);
                }
            }
        });
    }

    protected static XContentBuilder convertToJson(RestChannel channel, ToXContent toxContent) {
        try {
            XContentBuilder builder = channel.newBuilder();
            toxContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder;
        } catch (IOException e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    protected void response(RestChannel channel, RestStatus status, String message) {

        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("status", status.name());
            builder.field("message", message);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    protected void successResponse(RestChannel channel, SecurityDynamicConfiguration<?> response) {
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, convertToJson(channel, response)));
    }

    protected void successResponse(RestChannel channel) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (IOException e) {
            internalErrorResponse(channel, "Unable to fetch license: " + e.getMessage());
            log.error("Cannot fetch convert license to XContent due to", e);
        }
    }

    protected void badRequestResponse(RestChannel channel, AbstractConfigurationValidator validator) {
        channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, validator.errorsAsXContent(channel)));
    }

    protected void successResponse(RestChannel channel, String message) {
        response(channel, RestStatus.OK, message);
    }

    protected void createdResponse(RestChannel channel, String message) {
        response(channel, RestStatus.CREATED, message);
    }

    protected void badRequestResponse(RestChannel channel, String message) {
        response(channel, RestStatus.BAD_REQUEST, message);
    }

    protected void notFound(RestChannel channel, String message) {
        response(channel, RestStatus.NOT_FOUND, message);
    }

    protected void forbidden(RestChannel channel, String message) {
        response(channel, RestStatus.FORBIDDEN, message);
    }

    protected void internalErrorResponse(RestChannel channel, String message) {
        response(channel, RestStatus.INTERNAL_SERVER_ERROR, message);
    }

    protected void unprocessable(RestChannel channel, String message) {
        response(channel, RestStatus.UNPROCESSABLE_ENTITY, message);
    }

    protected void conflict(RestChannel channel, String message) {
        response(channel, RestStatus.CONFLICT, message);
    }

    protected void notImplemented(RestChannel channel, Method method) {
        response(channel, RestStatus.NOT_IMPLEMENTED, "Method " + method.name() + " not supported for this action.");
    }

    /**
     * Consume all defined parameters for the request. Before we handle the
     * request in subclasses where we actually need the parameter, some global
     * checks are performed, e.g. check whether the .security_index index exists. Thus, the
     * parameter(s) have not been consumed, and OpenSearch will always return a 400 with
     * an internal error message.
     *
     * @param request
     */
    protected void consumeParameters(final RestRequest request) {
        request.param("name");
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    protected abstract Endpoint getEndpoint();

    protected boolean isSuperAdmin() {
        // User user = threadPool.getThreadContext().getTransient(ConfigConstants.IDENTITY_USER);
        // TODO implement this by creating something similar to AdminDNS.java (see security repo)
        return false;
    }
}
