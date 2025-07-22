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

package org.opensearch.common.xcontent;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.xcontent.ErrorOnUnknown;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.stream.Collectors.toList;

/**
 * Utility for suggesting source of errors.
 *
 * @opensearch.internal
 */
public class SuggestingErrorOnUnknown implements ErrorOnUnknown {
    @Override
    public String errorMessage(String parserName, String unknownField, Iterable<String> candidates) {
        return String.format(Locale.ROOT, "[%s] unknown field [%s]%s", parserName, unknownField, suggest(unknownField, candidates));
    }

    @Override
    public int priority() {
        return 0;
    }

    /**
     * Builds suggestions for an unknown field, returning an empty string if there
     * aren't any suggestions or " did you mean " and then the list of suggestions.
     */
    public static String suggest(String unknownField, Iterable<String> candidates) {
        // TODO it'd be nice to combine this with BaseRestHandler's implementation.
        LevenshteinDistance ld = new LevenshteinDistance();
        final List<Tuple<Float, String>> scored = new ArrayList<>();
        for (String candidate : candidates) {
            float distance = ld.getDistance(unknownField, candidate);
            if (distance > 0.5f) {
                scored.add(new Tuple<>(distance, candidate));
            }
        }
        if (scored.isEmpty()) {
            return "";
        }
        CollectionUtil.timSort(scored, (a, b) -> {
            // sort by distance in reverse order, then parameter name for equal distances
            int compare = a.v1().compareTo(b.v1());
            if (compare != 0) {
                return -compare;
            }
            return a.v2().compareTo(b.v2());
        });
        List<String> keys = scored.stream().map(Tuple::v2).collect(toList());
        StringBuilder builder = new StringBuilder(" did you mean ");
        if (keys.size() == 1) {
            builder.append("[").append(keys.get(0)).append("]");
        } else {
            builder.append("any of ").append(keys.toString());
        }
        builder.append("?");
        return builder.toString();
    }
}
