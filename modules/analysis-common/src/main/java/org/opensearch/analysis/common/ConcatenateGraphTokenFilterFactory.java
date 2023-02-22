/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;

/**
 * Factory for {@link ConcatenateGraphFilter}.
 * Adopted from {@link org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilterFactory}, with some changes to
 * default values: token_separator is a "space", preserve_position_increments is false to avoid duplicated separators,
 * max_graph_expansions is 100 as the default value of 10_000 seems to be unnecessarily large and preserve_separator is false.
 *
 * <ul>
 *   <li>token_separator:
 *       Separator to use for concatenation. Must be a String with a single character or empty.
 *       If not present, {@link ConcatenateGraphTokenFilterFactory#DEFAULT_TOKEN_SEPARATOR} will be used.
 *       If empty i.e. "", tokens will be concatenated without any separators.
 *       </li>
 *   <li>preserve_position_increments:
 *       Whether to add an empty token for missing positions.
 *       If not present, {@link ConcatenateGraphTokenFilterFactory#DEFAULT_PRESERVE_POSITION_INCREMENTS} will be used.
 *       </li>
 *   <li>max_graph_expansions:
 *       If the tokenStream graph has more than this many possible paths through, then we'll throw
 *       {@link TooComplexToDeterminizeException} to preserve the stability and memory of the
 *       machine.
 *       If not present, {@link ConcatenateGraphTokenFilterFactory#DEFAULT_MAX_GRAPH_EXPANSIONS} will be used.
 *       </li>
 * </ul>
 * @see ConcatenateGraphFilter
 */
public class ConcatenateGraphTokenFilterFactory extends AbstractTokenFilterFactory {
    public static final String DEFAULT_TOKEN_SEPARATOR = " ";
    public static final int DEFAULT_MAX_GRAPH_EXPANSIONS = 100;
    public static final boolean DEFAULT_PRESERVE_POSITION_INCREMENTS = false;

    private final Character tokenSeparator;
    private final int maxGraphExpansions;
    private final boolean preservePositionIncrements;

    ConcatenateGraphTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        String separator = settings.get("token_separator", DEFAULT_TOKEN_SEPARATOR);
        if (separator.length() > 1) {
            throw new IllegalArgumentException("token_separator must be either empty or a single character");
        }
        tokenSeparator = separator.length() == 0 ? null : separator.charAt(0); // null means no separator while concatenating
        maxGraphExpansions = settings.getAsInt("max_graph_expansions", DEFAULT_MAX_GRAPH_EXPANSIONS);
        preservePositionIncrements = settings.getAsBoolean("preserve_position_increments", DEFAULT_PRESERVE_POSITION_INCREMENTS);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ConcatenateGraphFilter(tokenStream, tokenSeparator, preservePositionIncrements, maxGraphExpansions);
    }
}
