/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.suggest;

import org.opensearch.search.suggest.term.TermSuggestionBuilder;

/**
 * Utility class for converting TermSuggestionBuilder components between OpenSearch and Protocol Buffers formats.
 * This class provides methods to transform suggestion modes and other term suggestion parameters
 * to ensure proper handling of term suggestions in search operations.
 */
public class TermSuggestionBuilderProtoUtils {
    private TermSuggestionBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Resolves a Protocol Buffer SuggestMode to a TermSuggestionBuilder.SuggestMode.
     * Similar to {@link TermSuggestionBuilder.SuggestMode#resolve(String)}
     *
     * @param suggest_mode the Protocol Buffer SuggestMode to resolve
     * @return the corresponding TermSuggestionBuilder.SuggestMode
     * @throws IllegalArgumentException if the suggest_mode is invalid
     */
    public static TermSuggestionBuilder.SuggestMode resolve(final org.opensearch.protobufs.SuggestMode suggest_mode) {
        switch (suggest_mode) {
            case SUGGEST_MODE_ALWAYS:
                return TermSuggestionBuilder.SuggestMode.ALWAYS;
            case SUGGEST_MODE_MISSING:
                return TermSuggestionBuilder.SuggestMode.MISSING;
            case SUGGEST_MODE_POPULAR:
                return TermSuggestionBuilder.SuggestMode.POPULAR;
            default:
                throw new IllegalArgumentException("Invalid suggest_mode " + suggest_mode.toString());
        }
    }
}
