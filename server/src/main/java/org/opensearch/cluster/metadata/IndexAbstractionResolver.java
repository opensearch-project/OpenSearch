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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.metadata;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class to resolve index abstractions
 *
 * @opensearch.internal
 */
public class IndexAbstractionResolver {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexAbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public List<String> resolveIndexAbstractions(
        String[] indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams
    ) {
        return resolveIndexAbstractions(indices, indicesOptions, metadata, includeDataStreams, true);
    }

    public List<String> resolveIndexAbstractions(
        String[] indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams,
        boolean throwExceptions
    ) {
        return resolveIndexAbstractions(Arrays.asList(indices), indicesOptions, metadata, includeDataStreams, throwExceptions);
    }

    public List<String> resolveIndexAbstractions(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams,
        boolean throwExceptions
    ) {
        final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();
        Set<String> availableIndexAbstractions = metadata.getIndicesLookup().keySet();
        return resolveIndexAbstractions(
            indices,
            indicesOptions,
            metadata,
            availableIndexAbstractions,
            replaceWildcards,
            includeDataStreams,
            throwExceptions
        );
    }

    public List<String> resolveIndexAbstractions(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        Collection<String> availableIndexAbstractions,
        boolean replaceWildcards,
        boolean includeDataStreams,
        boolean throwExceptions
    ) {
        List<String> finalIndices = new ArrayList<>();
        boolean wildcardSeen = false;
        for (String index : indices) {
            String indexAbstraction;
            boolean minus = false;
            if (index.charAt(0) == '-' && wildcardSeen) {
                indexAbstraction = index.substring(1);
                minus = true;
            } else {
                indexAbstraction = index;
            }

            // we always need to check for date math expressions
            final String dateMathName = indexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);
            if (dateMathName != indexAbstraction) {
                assert dateMathName.equals(indexAbstraction) == false;
                if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                    // continue
                    indexAbstraction = dateMathName;
                } else if (availableIndexAbstractions.contains(dateMathName)) {
                    if (isIndexVisible(indexAbstraction, dateMathName, indicesOptions, metadata, includeDataStreams, true)) {
                        if (minus) {
                            finalIndices.remove(dateMathName);
                        } else {
                            finalIndices.add(dateMathName);
                        }
                    } else if (throwExceptions) {
                        if (indicesOptions.ignoreUnavailable() == false) {
                            throw new IndexNotFoundException(dateMathName);
                        }
                    }
                } else {
                    if (!throwExceptions) {
                        if (minus) {
                            finalIndices.remove(dateMathName);
                        } else {
                            finalIndices.add(dateMathName);
                        }
                    } else if (indicesOptions.ignoreUnavailable() == false) {
                        throw new IndexNotFoundException(dateMathName);
                    }
                }
            }

            if (replaceWildcards && Regex.isSimpleMatchPattern(indexAbstraction)) {
                wildcardSeen = true;
                Set<String> resolvedIndices = new HashSet<>();
                for (String authorizedIndex : availableIndexAbstractions) {
                    if (Regex.simpleMatch(indexAbstraction, authorizedIndex)
                        && isIndexVisible(indexAbstraction, authorizedIndex, indicesOptions, metadata, includeDataStreams)) {
                        resolvedIndices.add(authorizedIndex);
                    }
                }
                if (resolvedIndices.isEmpty()) {
                    // es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                    if (indicesOptions.allowNoIndices() == false && throwExceptions) {
                        throw new IndexNotFoundException(indexAbstraction);
                    }
                } else {
                    if (minus) {
                        finalIndices.removeAll(resolvedIndices);
                    } else {
                        finalIndices.addAll(resolvedIndices);
                    }
                }
            } else if (dateMathName.equals(indexAbstraction)) {
                if (minus) {
                    finalIndices.remove(indexAbstraction);
                } else {
                    finalIndices.add(indexAbstraction);
                }
            }
        }
        return finalIndices;
    }

    public static boolean isIndexVisible(
        String expression,
        String index,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams
    ) {
        return isIndexVisible(expression, index, indicesOptions, metadata, includeDataStreams, false);
    }

    public static boolean isIndexVisible(
        String expression,
        String index,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams,
        boolean dateMathExpression
    ) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
        if (indexAbstraction == null) {
            throw new IllegalStateException("could not resolve index abstraction [" + index + "]");
        }
        final boolean isHidden = indexAbstraction.isHidden();
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            // it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            // complicated to support those options with aliases pointing to multiple indices...
            if (indicesOptions.ignoreAliases()) {
                return false;
            } else if (isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index)) {
                return true;
            } else {
                return false;
            }
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
            return includeDataStreams;
        }
        assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
        // since it is a date math expression, we consider the index visible regardless of open/closed/hidden as the user is using
        // date math to explicitly reference the index
        if (dateMathExpression) {
            assert IndexMetadata.State.values().length == 2 : "a new IndexMetadata.State value may need to be handled!";
            return true;
        }
        if (isHidden && indicesOptions.expandWildcardsHidden() == false && isVisibleDueToImplicitHidden(expression, index) == false) {
            return false;
        }

        IndexMetadata indexMetadata = indexAbstraction.getIndices().get(0);
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
            return true;
        }
        return false;
    }

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }
}
