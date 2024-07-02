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

package org.opensearch.core.xcontent.filtering;

import org.opensearch.common.Glob;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Filters a content path
 *
 * @opensearch.internal
 */
public class FilterPath {

    static final FilterPath EMPTY = new FilterPath();
    private final String filter;
    private final String segment;
    private final FilterPath next;
    private final boolean simpleWildcard;
    private final boolean doubleWildcard;

    protected FilterPath(String filter, String segment, FilterPath next) {
        this.filter = filter;
        this.segment = segment;
        this.next = next;
        this.simpleWildcard = (segment != null) && (segment.length() == 1) && (segment.charAt(0) == '*');
        this.doubleWildcard = (segment != null) && (segment.length() == 2) && (segment.charAt(0) == '*') && (segment.charAt(1) == '*');
    }

    private FilterPath() {
        this("<empty>", "", null);
    }

    public FilterPath matchProperty(String name) {
        if ((next != null) && (simpleWildcard || doubleWildcard || Glob.globMatch(segment, name))) {
            return next;
        }
        return null;
    }

    public boolean matches() {
        return next == null;
    }

    boolean isDoubleWildcard() {
        return doubleWildcard;
    }

    boolean isSimpleWildcard() {
        return simpleWildcard;
    }

    String getSegment() {
        return segment;
    }

    FilterPath getNext() {
        return next;
    }

    public static FilterPath[] compile(Set<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        List<FilterPath> paths = new ArrayList<>();
        for (String filter : filters) {
            if (filter != null && !filter.isEmpty()) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    paths.add(parse(filter));
                }
            }
        }
        return paths.toArray(new FilterPath[paths.size()]);
    }

    private static FilterPath parse(final String filter) {
        // Split the filter into segments using a regex
        // that avoids splitting escaped dots.
        String[] segments = filter.split("(?<!\\\\)\\.");
        FilterPath next = EMPTY;

        for (int i = segments.length - 1; i >= 0; i--) {
            // Replace escaped dots with actual dots in the current segment.
            String segment = segments[i].replaceAll("\\\\.", ".");
            next = new FilterPath(filter, segment, next);
        }

        return next;
    }

    @Override
    public String toString() {
        return "FilterPath [filter=" + filter + ", segment=" + segment + "]";
    }
}
