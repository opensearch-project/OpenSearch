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

package org.opensearch.rest.action.admin.indices;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.HEAD;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.context.Scope;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.HEAD;

/**
 * The REST handler for get alias and head alias APIs.
 */
public class RestGetAliasesAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestGetAliasesAction.class);


    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_alias"),
                new Route(GET, "/_aliases"),
                new Route(GET, "/_alias/{name}"),
                new Route(HEAD, "/_alias/{name}"),
                new Route(GET, "/{index}/_alias"),
                new Route(HEAD, "/{index}/_alias"),
                new Route(GET, "/{index}/_alias/{name}"),
                new Route(HEAD, "/{index}/_alias/{name}")
            )
        );
    }

    @Override
    public String getName() {
        return "get_aliases_action";
    }

    static RestResponse buildRestResponse(
        boolean aliasesExplicitlyRequested,
        String[] requestedAliases,
        ImmutableOpenMap<String, List<AliasMetadata>> responseAliasMap,
        XContentBuilder builder
    ) throws Exception {
        final Set<String> indicesToDisplay = new HashSet<>();
        final Set<String> returnedAliasNames = new HashSet<>();
        for (final ObjectObjectCursor<String, List<AliasMetadata>> cursor : responseAliasMap) {
            for (final AliasMetadata aliasMetadata : cursor.value) {
                if (aliasesExplicitlyRequested) {
                    // only display indices that have aliases
                    indicesToDisplay.add(cursor.key);
                }
                returnedAliasNames.add(aliasMetadata.alias());
            }
        }
        // compute explicitly requested aliases that have are not returned in the result
        final SortedSet<String> missingAliases = new TreeSet<>();
        // first wildcard index, leading "-" as an alias name after this index means
        // that it is an exclusion
        int firstWildcardIndex = requestedAliases.length;
        for (int i = 0; i < requestedAliases.length; i++) {
            if (Regex.isSimpleMatchPattern(requestedAliases[i])) {
                firstWildcardIndex = i;
                break;
            }
        }
        for (int i = 0; i < requestedAliases.length; i++) {
            if (Metadata.ALL.equals(requestedAliases[i])
                || Regex.isSimpleMatchPattern(requestedAliases[i])
                || (i > firstWildcardIndex && requestedAliases[i].charAt(0) == '-')) {
                // only explicitly requested aliases will be called out as missing (404)
                continue;
            }
            // check if aliases[i] is subsequently excluded
            int j = Math.max(i + 1, firstWildcardIndex);
            for (; j < requestedAliases.length; j++) {
                if (requestedAliases[j].charAt(0) == '-') {
                    // this is an exclude pattern
                    if (Regex.simpleMatch(requestedAliases[j].substring(1), requestedAliases[i])
                        || Metadata.ALL.equals(requestedAliases[j].substring(1))) {
                        // aliases[i] is excluded by aliases[j]
                        break;
                    }
                }
            }
            if (j == requestedAliases.length) {
                // explicitly requested aliases[i] is not excluded by any subsequent "-" wildcard in expression
                if (false == returnedAliasNames.contains(requestedAliases[i])) {
                    // aliases[i] is not in the result set
                    missingAliases.add(requestedAliases[i]);
                }
            }
        }

        final RestStatus status;
        builder.startObject();
        {
            if (missingAliases.isEmpty()) {
                status = RestStatus.OK;
            } else {
                status = RestStatus.NOT_FOUND;
                final String message;
                if (missingAliases.size() == 1) {
                    message = String.format(Locale.ROOT, "alias [%s] missing", Strings.collectionToCommaDelimitedString(missingAliases));
                } else {
                    message = String.format(Locale.ROOT, "aliases [%s] missing", Strings.collectionToCommaDelimitedString(missingAliases));
                }
                builder.field("error", message);
                builder.field("status", status.getStatus());
            }

            for (final ObjectObjectCursor<String, List<AliasMetadata>> entry : responseAliasMap) {
                if (aliasesExplicitlyRequested == false || (aliasesExplicitlyRequested && indicesToDisplay.contains(entry.key))) {
                    builder.startObject(entry.key);
                    {
                        builder.startObject("aliases");
                        {
                            for (final AliasMetadata alias : entry.value) {
                                AliasMetadata.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                            }
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }
        }
        builder.endObject();
        return new BytesRestResponse(status, builder);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // The TransportGetAliasesAction was improved do the same post processing as is happening here.
        // We can't remove this logic yet to support mixed clusters. We should be able to remove this logic here
        // in when 8.0 becomes the new version in the master branch.

        logger.info("mm7->>");

        /*Resource resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "logical-service-name")));

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder()
                .setTimeout(2, TimeUnit.SECONDS).build()).build())
            .setResource(resource)
            .build();

        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(OtlpGrpcMetricExporter.builder().build()).build())
            .setResource(resource)
            .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
*/
        Tracer tracer =
            openTelemetry.getTracer("instrumentation-library-name", "1.0.0");
        Span span = tracer.spanBuilder("my span").startSpan();

// Make the span the current span
        try (Scope ss = span.makeCurrent()) {
            span.setAttribute("http.method", "GET");
            span.setAttribute("http.url", "mm55");
        } finally {
            span.end();
        }
        final boolean namesProvided = request.hasParam("name");
        final String[] aliases = request.paramAsStringArrayOrEmptyIfAll("name");
        final GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliases);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        // we may want to move this logic to TransportGetAliasesAction but it is based on the original provided aliases, which will
        // not always be available there (they may get replaced so retrieving request.aliases is not quite the same).
        return channel -> client.admin().indices().getAliases(getAliasesRequest, new RestBuilderListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response, XContentBuilder builder) throws Exception {
                return buildRestResponse(namesProvided, aliases, response.getAliases(), builder);
            }
        });
    }

    public static final OpenTelemetry openTelemetry;

    static {
        Resource resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch-Search")));

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder()
                .setTimeout(2, TimeUnit.SECONDS).build()).build())
//            .addSpanProcessor(SimpleSpanProcessor.create(OtlpGrpcSpanExporter.builder()
//                .setTimeout(2, TimeUnit.SECONDS).build()))
            .setResource(resource)
            .build();

        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(OtlpGrpcMetricExporter.builder().build()).build())
            .setResource(resource)
            .build();

        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
//            .setMeterProvider(sdkMeterProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
    }

}
