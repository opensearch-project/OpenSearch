/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.ingest.ConfigurationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.TAG_KEY;
import static org.opensearch.ingest.Pipeline.DESCRIPTION_KEY;
import static org.opensearch.ingest.Pipeline.VERSION_KEY;

/**
 * Concrete representation of a search pipeline, holding multiple processors.
 */
class Pipeline {

    public static final String REQUEST_PROCESSORS_KEY = "request_processors";
    public static final String RESPONSE_PROCESSORS_KEY = "response_processors";
    private final String id;
    private final String description;
    private final Integer version;

    // TODO: Refactor org.opensearch.ingest.CompoundProcessor to implement our generic Processor interface
    // Then these can be CompoundProcessors instead of lists.
    private final List<SearchRequestProcessor> searchRequestProcessors;
    private final List<SearchResponseProcessor> searchResponseProcessors;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private Pipeline(
        String id,
        @Nullable String description,
        @Nullable Integer version,
        List<SearchRequestProcessor> requestProcessors,
        List<SearchResponseProcessor> responseProcessors,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.searchRequestProcessors = requestProcessors;
        this.searchResponseProcessors = responseProcessors;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    static Pipeline create(
        String id,
        Map<String, Object> config,
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorFactories,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorFactories,
        NamedWriteableRegistry namedWriteableRegistry
    ) throws Exception {
        String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
        Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
        List<Map<String, Object>> requestProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, REQUEST_PROCESSORS_KEY);
        List<SearchRequestProcessor> requestProcessors = readProcessors(requestProcessorFactories, requestProcessorConfigs);
        List<Map<String, Object>> responseProcessorConfigs = ConfigurationUtils.readOptionalList(
            null,
            null,
            config,
            RESPONSE_PROCESSORS_KEY
        );
        List<SearchResponseProcessor> responseProcessors = readProcessors(responseProcessorFactories, responseProcessorConfigs);
        if (config.isEmpty() == false) {
            throw new OpenSearchParseException(
                "pipeline ["
                    + id
                    + "] doesn't support one or more provided configuration parameters "
                    + Arrays.toString(config.keySet().toArray())
            );
        }
        return new Pipeline(id, description, version, requestProcessors, responseProcessors, namedWriteableRegistry);
    }

    private static <T extends Processor> List<T> readProcessors(
        Map<String, Processor.Factory<T>> processorFactories,
        List<Map<String, Object>> requestProcessorConfigs
    ) throws Exception {
        List<T> processors = new ArrayList<>();
        if (requestProcessorConfigs == null) {
            return processors;
        }
        for (Map<String, Object> processorConfigWithKey : requestProcessorConfigs) {
            for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                String type = entry.getKey();
                if (!processorFactories.containsKey(type)) {
                    throw new IllegalArgumentException("Invalid processor type " + type);
                }
                Map<String, Object> config = (Map<String, Object>) entry.getValue();
                String tag = ConfigurationUtils.readOptionalStringProperty(null, null, config, TAG_KEY);
                String description = ConfigurationUtils.readOptionalStringProperty(null, tag, config, DESCRIPTION_KEY);
                processors.add(processorFactories.get(type).create(processorFactories, tag, description, config));
            }
        }
        return Collections.unmodifiableList(processors);
    }

    String getId() {
        return id;
    }

    String getDescription() {
        return description;
    }

    Integer getVersion() {
        return version;
    }

    List<SearchRequestProcessor> getSearchRequestProcessors() {
        return searchRequestProcessors;
    }

    List<SearchResponseProcessor> getSearchResponseProcessors() {
        return searchResponseProcessors;
    }

    SearchRequest transformRequest(SearchRequest request) throws Exception {
        if (searchRequestProcessors.isEmpty() == false) {
            try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
                request.writeTo(bytesStreamOutput);
                try (StreamInput in = bytesStreamOutput.bytes().streamInput()) {
                    try (StreamInput input = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry)) {
                        request = new SearchRequest(input);
                    }
                }
            }
            for (SearchRequestProcessor searchRequestProcessor : searchRequestProcessors) {
                request = searchRequestProcessor.processRequest(request);
            }
        }
        return request;
    }

    SearchResponse transformResponse(SearchRequest request, SearchResponse response) throws SearchPipelineProcessingException {
        try {
            for (SearchResponseProcessor responseProcessor : searchResponseProcessors) {
                response = responseProcessor.processResponse(request, response);
            }
            return response;
        } catch (Exception e) {
            throw new SearchPipelineProcessingException(e);
        }
    }

    static final Pipeline NO_OP_PIPELINE = new Pipeline(
        SearchPipelineService.NOOP_PIPELINE_ID,
        "Pipeline that does not transform anything",
        0,
        Collections.emptyList(),
        Collections.emptyList(),
        null
    );
}
