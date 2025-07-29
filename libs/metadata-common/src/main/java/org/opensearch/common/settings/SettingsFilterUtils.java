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

package org.opensearch.common.settings;

import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent.Params;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that provides settings filtering functionality.
 *
 * @opensearch.internal
 */
public final class SettingsFilterUtils {
    /**
     * Can be used to specify settings filter that will be used to filter out matching settings in toXContent method
     */
    public static String SETTINGS_FILTER_PARAM = "settings_filter";

    private SettingsFilterUtils() {}

    public static Settings filterSettings(Params params, Settings settings) {
        String patterns = params.param(SETTINGS_FILTER_PARAM);
        final Settings filteredSettings;
        if (patterns != null && patterns.isEmpty() == false) {
            filteredSettings = filterSettings(Strings.commaDelimitedListToSet(patterns), settings);
        } else {
            filteredSettings = settings;
        }
        return filteredSettings;
    }

    public static Settings filterSettings(Iterable<String> patterns, Settings settings) {
        Settings.Builder builder = Settings.builder().put(settings);
        List<String> simpleMatchPatternList = new ArrayList<>();
        for (String pattern : patterns) {
            if (Regex.isSimpleMatchPattern(pattern)) {
                simpleMatchPatternList.add(pattern);
            } else {
                builder.remove(pattern);
            }
        }
        if (!simpleMatchPatternList.isEmpty()) {
            String[] simpleMatchPatterns = simpleMatchPatternList.toArray(new String[0]);
            builder.keys().removeIf(key -> Regex.simpleMatch(simpleMatchPatterns, key));
        }
        return builder.build();
    }
}
