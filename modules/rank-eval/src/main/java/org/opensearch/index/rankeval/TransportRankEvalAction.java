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

package org.opensearch.index.rankeval;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.MultiSearchResponse.Item;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.common.xcontent.XContentHelper.createParser;
import static org.opensearch.index.rankeval.RatedRequest.validateEvaluatedQuery;

/**
 * Instances of this class execute a collection of search intents (read: user
 * supplied query parameters) against a set of possible search requests (read:
 * search specifications, expressed as query/search request templates) and
 * compares the result against a set of annotated documents per search intent.
 *
 * If any documents are returned that haven't been annotated the document id of
 * those is returned per search intent.
 *
 * The resulting search quality is computed in terms of precision at n and
 * returned for each search specification for the full set of search intents as
 * averaged precision at n.
 */
public class TransportRankEvalAction extends HandledTransportAction<RankEvalRequest, RankEvalResponse> {
    private final Client client;
    private final ScriptService scriptService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportRankEvalAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        NamedXContentRegistry namedXContentRegistry
    ) {
        super(RankEvalAction.NAME, transportService, actionFilters, (Writeable.Reader<RankEvalRequest>) RankEvalRequest::new);
        this.scriptService = scriptService;
        this.namedXContentRegistry = namedXContentRegistry;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RankEvalRequest request, ActionListener<RankEvalResponse> listener) {
        RankEvalSpec evaluationSpecification = request.getRankEvalSpec();
        EvaluationMetric metric = evaluationSpecification.getMetric();

        List<RatedRequest> ratedRequests = evaluationSpecification.getRatedRequests();
        Map<String, Exception> errors = new ConcurrentHashMap<>(ratedRequests.size());

        Map<String, TemplateScript.Factory> scriptsWithoutParams = new HashMap<>();
        for (Entry<String, Script> entry : evaluationSpecification.getTemplates().entrySet()) {
            scriptsWithoutParams.put(entry.getKey(), scriptService.compile(entry.getValue(), TemplateScript.CONTEXT));
        }

        MultiSearchRequest msearchRequest = new MultiSearchRequest();
        msearchRequest.maxConcurrentSearchRequests(evaluationSpecification.getMaxConcurrentSearches());
        List<RatedRequest> ratedRequestsInSearch = new ArrayList<>();
        for (RatedRequest ratedRequest : ratedRequests) {
            SearchSourceBuilder evaluationRequest = ratedRequest.getEvaluationRequest();
            if (evaluationRequest == null) {
                Map<String, Object> params = ratedRequest.getParams();
                String templateId = ratedRequest.getTemplateId();
                TemplateScript.Factory templateScript = scriptsWithoutParams.get(templateId);
                String resolvedRequest = templateScript.newInstance(params).execute();
                try (
                    XContentParser subParser = createParser(
                        namedXContentRegistry,
                        LoggingDeprecationHandler.INSTANCE,
                        new BytesArray(resolvedRequest),
                        XContentType.JSON
                    )
                ) {
                    evaluationRequest = SearchSourceBuilder.fromXContent(subParser, false);
                    // check for parts that should not be part of a ranking evaluation request
                    validateEvaluatedQuery(evaluationRequest);
                } catch (IOException e) {
                    // if we fail parsing, put the exception into the errors map and continue
                    errors.put(ratedRequest.getId(), e);
                    continue;
                }
            }

            if (metric.forcedSearchSize().isPresent()) {
                evaluationRequest.size(metric.forcedSearchSize().getAsInt());
            }

            ratedRequestsInSearch.add(ratedRequest);
            List<String> summaryFields = ratedRequest.getSummaryFields();
            if (summaryFields.isEmpty()) {
                evaluationRequest.fetchSource(false);
            } else {
                evaluationRequest.fetchSource(summaryFields.toArray(new String[0]), new String[0]);
            }
            SearchRequest searchRequest = new SearchRequest(request.indices(), evaluationRequest);
            searchRequest.indicesOptions(request.indicesOptions());
            searchRequest.searchType(request.searchType());
            msearchRequest.add(searchRequest);
        }
        assert ratedRequestsInSearch.size() == msearchRequest.requests().size();
        client.multiSearch(
            msearchRequest,
            new RankEvalActionListener(listener, metric, ratedRequestsInSearch.toArray(new RatedRequest[0]), errors)
        );
    }

    class RankEvalActionListener implements ActionListener<MultiSearchResponse> {

        private final ActionListener<RankEvalResponse> listener;
        private final RatedRequest[] specifications;

        private final Map<String, Exception> errors;
        private final EvaluationMetric metric;

        RankEvalActionListener(
            ActionListener<RankEvalResponse> listener,
            EvaluationMetric metric,
            RatedRequest[] specifications,
            Map<String, Exception> errors
        ) {
            this.listener = listener;
            this.metric = metric;
            this.errors = errors;
            this.specifications = specifications;
        }

        @Override
        public void onResponse(MultiSearchResponse multiSearchResponse) {
            int responsePosition = 0;
            Map<String, EvalQueryQuality> responseDetails = new HashMap<>(specifications.length);
            for (Item response : multiSearchResponse.getResponses()) {
                RatedRequest specification = specifications[responsePosition];
                if (response.isFailure() == false) {
                    SearchHit[] hits = response.getResponse().getHits().getHits();
                    EvalQueryQuality queryQuality = this.metric.evaluate(specification.getId(), hits, specification.getRatedDocs());
                    responseDetails.put(specification.getId(), queryQuality);
                } else {
                    errors.put(specification.getId(), response.getFailure());
                }
                responsePosition++;
            }
            listener.onResponse(new RankEvalResponse(this.metric.combine(responseDetails.values()), responseDetails, this.errors));
        }

        @Override
        public void onFailure(Exception exception) {
            listener.onFailure(exception);
        }
    }
}
