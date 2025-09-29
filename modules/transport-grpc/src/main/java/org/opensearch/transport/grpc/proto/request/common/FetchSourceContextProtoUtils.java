/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.SourceConfig;
import org.opensearch.protobufs.SourceConfigParam;
import org.opensearch.protobufs.SourceFilter;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting SourceConfig Protocol Buffers to FetchSourceContext objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch objects.
 */
public class FetchSourceContextProtoUtils {

    private FetchSourceContextProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SourceConfig Protocol Buffer to a FetchSourceContext object.
     * Similar to {@link FetchSourceContext#parseFromRestRequest(RestRequest)}
     *
     * @param request The BulkRequest Protocol Buffer containing source configuration
     * @return A FetchSourceContext object based on the request parameters, or null if no source parameters are provided
     */
    public static FetchSourceContext parseFromProtoRequest(org.opensearch.protobufs.BulkRequest request) {
        Boolean fetchSource = true;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;

        // Set up source context if source parameters are provided
        if (request.hasXSource()) {
            switch (request.getXSource().getSourceConfigParamCase()) {
                case BOOL:
                    fetchSource = request.getXSource().getBool();
                    break;
                case STRING_ARRAY:
                    sourceIncludes = request.getXSource().getStringArray().getStringArrayList().toArray(new String[0]);
                    break;
                default:
                    throw new UnsupportedOperationException("Invalid sourceConfig provided.");
            }
        }

        if (request.getXSourceIncludesCount() > 0) {
            sourceIncludes = request.getXSourceIncludesList().toArray(new String[0]);
        }

        if (request.getXSourceExcludesCount() > 0) {
            sourceExcludes = request.getXSourceExcludesList().toArray(new String[0]);
        }
        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null) {
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    /**
     * Converts a SourceConfig Protocol Buffer to a FetchSourceContext object.
     * Similar to {@link FetchSourceContext#parseFromRestRequest(RestRequest)}
     *
     * @param request The SearchRequest Protocol Buffer containing source configuration
     * @return A FetchSourceContext object based on the request parameters, or null if no source parameters are provided
     */
    public static FetchSourceContext parseFromProtoRequest(org.opensearch.protobufs.SearchRequest request) {
        Boolean fetchSource = null;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;

        if (request.hasXSource()) {
            SourceConfigParam source = request.getXSource();

            if (source.hasBool()) {
                fetchSource = source.getBool();
            } else {
                sourceIncludes = source.getStringArray().getStringArrayList().toArray(new String[0]);
            }
        }

        if (request.getXSourceIncludesCount() > 0) {
            sourceIncludes = request.getXSourceIncludesList().toArray(new String[0]);
        }

        if (request.getXSourceExcludesCount() > 0) {
            sourceExcludes = request.getXSourceExcludesList().toArray(new String[0]);
        }

        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null) {
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    /**
     * Converts a SourceConfig Protocol Buffer to a FetchSourceContext object.
     * Similar to {@link FetchSourceContext#fromXContent(XContentParser)}.
     *
     * @param sourceConfig The SourceConfig Protocol Buffer to convert
     * @return A FetchSourceContext object
     */
    public static FetchSourceContext fromProto(SourceConfig sourceConfig) {
        boolean fetchSource = true;
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        if (sourceConfig.getSourceConfigCase() == SourceConfig.SourceConfigCase.FETCH) {
            fetchSource = sourceConfig.getFetch();
        } else if (sourceConfig.hasFilter()) {
            SourceFilter sourceFilter = sourceConfig.getFilter();
            if (sourceFilter.getIncludesCount() > 0) {
                List<String> includesList = new ArrayList<>();
                for (String s : sourceFilter.getIncludesList()) {
                    includesList.add(s);
                }
                includes = includesList.toArray(new String[0]);
            }
            if (sourceFilter.getExcludesCount() > 0) {
                List<String> excludesList = new ArrayList<>();
                for (String s : sourceFilter.getExcludesList()) {
                    excludesList.add(s);
                }
                excludes = excludesList.toArray(new String[0]);
            }
        }
        return new FetchSourceContext(fetchSource, includes, excludes);
    }
}
