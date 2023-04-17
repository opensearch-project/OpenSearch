/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.rules.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleAction;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleRequest;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleResponse;
import org.opensearch.plugin.correlation.rules.model.CorrelationRule;
import org.opensearch.plugin.correlation.utils.CorrelationRuleIndices;
import org.opensearch.plugin.correlation.utils.IndexUtils;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Locale;

/**
 * Transport Action for indexing correlation rules.
 *
 * @opensearch.internal
 */
public class TransportIndexCorrelationRuleAction extends HandledTransportAction<IndexCorrelationRuleRequest, IndexCorrelationRuleResponse> {

    private static final Logger log = LogManager.getLogger(TransportIndexCorrelationRuleAction.class);

    private final Client client;

    private final CorrelationRuleIndices correlationRuleIndices;

    private final ClusterService clusterService;

    /**
     * Parameterized ctor for Transport Action
     * @param transportService TransportService
     * @param client OS client
     * @param actionFilters ActionFilters
     * @param clusterService ClusterService
     * @param correlationRuleIndices CorrelationRuleIndices which manages lifecycle of correlation rule index
     */
    @Inject
    public TransportIndexCorrelationRuleAction(
        TransportService transportService,
        Client client,
        ActionFilters actionFilters,
        ClusterService clusterService,
        CorrelationRuleIndices correlationRuleIndices
    ) {
        super(IndexCorrelationRuleAction.NAME, transportService, actionFilters, IndexCorrelationRuleRequest::new);
        this.client = client;
        this.clusterService = clusterService;
        this.correlationRuleIndices = correlationRuleIndices;
    }

    @Override
    protected void doExecute(Task task, IndexCorrelationRuleRequest request, ActionListener<IndexCorrelationRuleResponse> listener) {
        AsyncIndexCorrelationRuleAction asyncAction = new AsyncIndexCorrelationRuleAction(request, listener);
        asyncAction.start();
    }

    private class AsyncIndexCorrelationRuleAction {
        private final IndexCorrelationRuleRequest request;

        private final ActionListener<IndexCorrelationRuleResponse> listener;

        AsyncIndexCorrelationRuleAction(IndexCorrelationRuleRequest request, ActionListener<IndexCorrelationRuleResponse> listener) {
            this.request = request;
            this.listener = listener;
        }

        void start() {
            try {
                if (correlationRuleIndices.correlationRuleIndexExists() == false) {
                    try {
                        correlationRuleIndices.initCorrelationRuleIndex(new ActionListener<>() {
                            @Override
                            public void onResponse(CreateIndexResponse response) {
                                try {
                                    onCreateMappingsResponse(response);
                                    indexCorrelationRule();
                                } catch (IOException e) {
                                    onFailures(e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                onFailures(e);
                            }
                        });
                    } catch (IOException e) {
                        onFailures(e);
                    }
                } else if (!IndexUtils.correlationRuleIndexUpdated) {
                    IndexUtils.updateIndexMapping(
                        CorrelationRule.CORRELATION_RULE_INDEX,
                        CorrelationRuleIndices.correlationRuleIndexMappings(),
                        clusterService.state(),
                        client.admin().indices(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(AcknowledgedResponse response) {
                                onUpdateMappingsResponse(response);
                                try {
                                    indexCorrelationRule();
                                } catch (IOException e) {
                                    onFailures(e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                onFailures(e);
                            }
                        }
                    );
                } else {
                    indexCorrelationRule();
                }
            } catch (IOException ex) {
                onFailures(ex);
            }
        }

        void indexCorrelationRule() throws IOException {
            IndexRequest indexRequest;
            if (request.getMethod() == RestRequest.Method.POST) {
                indexRequest = new IndexRequest(CorrelationRule.CORRELATION_RULE_INDEX).setRefreshPolicy(
                    WriteRequest.RefreshPolicy.IMMEDIATE
                )
                    .source(request.getCorrelationRule().toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .timeout(TimeValue.timeValueSeconds(60));
            } else {
                indexRequest = new IndexRequest(CorrelationRule.CORRELATION_RULE_INDEX).setRefreshPolicy(
                    WriteRequest.RefreshPolicy.IMMEDIATE
                )
                    .source(request.getCorrelationRule().toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .id(request.getCorrelationRuleId())
                    .timeout(TimeValue.timeValueSeconds(60));
            }

            client.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse response) {
                    if (response.status().equals(RestStatus.CREATED) || response.status().equals(RestStatus.OK)) {
                        CorrelationRule ruleResponse = request.getCorrelationRule();
                        ruleResponse.setId(response.getId());
                        onOperation(ruleResponse);
                    } else {
                        onFailures(new OpenSearchStatusException(response.toString(), RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    onFailures(e);
                }
            });
        }

        private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
            if (response.isAcknowledged()) {
                log.info(String.format(Locale.ROOT, "Created %s with mappings.", CorrelationRule.CORRELATION_RULE_INDEX));
                IndexUtils.correlationRuleIndexUpdated();
            } else {
                log.error(String.format(Locale.ROOT, "Create %s mappings call not acknowledged.", CorrelationRule.CORRELATION_RULE_INDEX));
                throw new OpenSearchStatusException(
                    String.format(Locale.getDefault(), "Create %s mappings call not acknowledged", CorrelationRule.CORRELATION_RULE_INDEX),
                    RestStatus.INTERNAL_SERVER_ERROR
                );
            }
        }

        private void onUpdateMappingsResponse(AcknowledgedResponse response) {
            if (response.isAcknowledged()) {
                log.info(String.format(Locale.ROOT, "Created %s with mappings.", CorrelationRule.CORRELATION_RULE_INDEX));
                IndexUtils.correlationRuleIndexUpdated();
            } else {
                log.error(String.format(Locale.ROOT, "Create %s mappings call not acknowledged.", CorrelationRule.CORRELATION_RULE_INDEX));
                throw new OpenSearchStatusException(
                    String.format(Locale.getDefault(), "Create %s mappings call not acknowledged", CorrelationRule.CORRELATION_RULE_INDEX),
                    RestStatus.INTERNAL_SERVER_ERROR
                );
            }
        }

        private void onOperation(CorrelationRule correlationRule) {
            finishHim(correlationRule, null);
        }

        private void onFailures(Exception t) {
            finishHim(null, t);
        }

        private void finishHim(CorrelationRule correlationRule, Exception t) {
            if (t != null) {
                listener.onFailure(t);
            } else {
                listener.onResponse(
                    new IndexCorrelationRuleResponse(
                        correlationRule.getId(),
                        correlationRule.getVersion(),
                        request.getMethod() == RestRequest.Method.POST ? RestStatus.CREATED : RestStatus.OK,
                        correlationRule
                    )
                );
            }
        }
    }
}
