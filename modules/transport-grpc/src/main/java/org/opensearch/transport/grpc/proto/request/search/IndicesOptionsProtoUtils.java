/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.rest.RestRequest;

import java.util.EnumSet;
import java.util.List;

import static org.opensearch.action.support.IndicesOptions.WildcardStates.CLOSED;
import static org.opensearch.action.support.IndicesOptions.WildcardStates.HIDDEN;
import static org.opensearch.action.support.IndicesOptions.WildcardStates.OPEN;
import static org.opensearch.action.support.IndicesOptions.fromOptions;

/**
 * Utility class for converting IndicesOptions between OpenSearch and Protocol Buffers formats.
 * This class provides methods to extract and transform indices options from Protocol Buffer requests
 * to ensure proper handling of index wildcards, unavailable indices, and other index-related settings.
 */
public class IndicesOptionsProtoUtils {

    private IndicesOptionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Extracts indices options from a Protocol Buffer SearchRequest.
     * Similar to {@link IndicesOptions#fromRequest(RestRequest, IndicesOptions)}
     *
     * @param request the Protocol Buffer SearchRequest to extract options from
     * @param defaultSettings the default IndicesOptions to use if not specified in the request
     * @return the IndicesOptions based on the request parameters
     */
    protected static IndicesOptions fromRequest(org.opensearch.protobufs.SearchRequest request, IndicesOptions defaultSettings) {
        return fromProtoParameters(request, defaultSettings);
    }

    /**
     * Creates IndicesOptions from Protocol Buffer SearchRequest parameters.
     * Similar to {@link IndicesOptions#fromParameters(Object, Object, Object, Object, IndicesOptions)}
     *
     * @param request the Protocol Buffer SearchRequest to extract parameters from
     * @param defaultSettings the default IndicesOptions to use if not specified in the request
     * @return the IndicesOptions based on the request parameters
     */
    protected static IndicesOptions fromProtoParameters(SearchRequest request, IndicesOptions defaultSettings) {
        if (!(request.getExpandWildcardsCount() > 0)
            && !request.hasIgnoreUnavailable()
            && !request.hasAllowNoIndices()
            && !request.hasIgnoreThrottled()) {
            return defaultSettings;
        }

        // TODO double check this works
        EnumSet<IndicesOptions.WildcardStates> wildcards = parseProtoParameter(
            request.getExpandWildcardsList(),
            defaultSettings.getExpandWildcards()
        );

        // note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return fromOptions(
            request.hasIgnoreUnavailable() ? request.getIgnoreUnavailable() : defaultSettings.ignoreUnavailable(),
            request.hasAllowNoIndices() ? request.getAllowNoIndices() : defaultSettings.allowNoIndices(),
            wildcards.contains(OPEN),
            wildcards.contains(CLOSED),
            wildcards.contains(IndicesOptions.WildcardStates.HIDDEN),
            defaultSettings.allowAliasesToMultipleIndices(),
            defaultSettings.forbidClosedIndices(),
            defaultSettings.ignoreAliases(),
            request.hasIgnoreThrottled() ? request.getIgnoreThrottled() : defaultSettings.ignoreThrottled()
        );
    }

    /**
     * Parses a list of ExpandWildcard values into an EnumSet of WildcardStates.
     * Similar to {@link IndicesOptions.WildcardStates#parseParameter(Object, EnumSet)}
     *
     * @param wildcardList the list of ExpandWildcard values to parse
     * @param defaultStates the default WildcardStates to use if the list is empty
     * @return an EnumSet of WildcardStates based on the provided wildcardList
     */
    protected static EnumSet<IndicesOptions.WildcardStates> parseProtoParameter(
        List<org.opensearch.protobufs.ExpandWildcard> wildcardList,
        EnumSet<IndicesOptions.WildcardStates> defaultStates
    ) {
        if (wildcardList.isEmpty()) {
            return defaultStates;
        }

        EnumSet<IndicesOptions.WildcardStates> states = EnumSet.noneOf(IndicesOptions.WildcardStates.class);
        for (org.opensearch.protobufs.ExpandWildcard wildcard : wildcardList) {
            updateSetForValue(states, wildcard);
        }

        return states;
    }

    /**
     * Updates an EnumSet of WildcardStates based on the provided ExpandWildcard value.
     * Keep implementation consistent with {@link IndicesOptions.WildcardStates#updateSetForValue(EnumSet, String)}
     *
     * @param states the EnumSet of WildcardStates to update
     * @param wildcard the ExpandWildcard value to use for updating the states
     */
    protected static void updateSetForValue(
        EnumSet<IndicesOptions.WildcardStates> states,
        org.opensearch.protobufs.ExpandWildcard wildcard
    ) {
        switch (wildcard) {
            case EXPAND_WILDCARD_OPEN:
                states.add(OPEN);
                break;
            case EXPAND_WILDCARD_CLOSED:
                states.add(CLOSED);
                break;
            case EXPAND_WILDCARD_HIDDEN:
                states.add(HIDDEN);
                break;
            case EXPAND_WILDCARD_NONE:
                states.clear();
                break;
            case EXPAND_WILDCARD_ALL:
                states.addAll(EnumSet.allOf(IndicesOptions.WildcardStates.class));
                break;
            default:
                throw new IllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
        }
    }
}
