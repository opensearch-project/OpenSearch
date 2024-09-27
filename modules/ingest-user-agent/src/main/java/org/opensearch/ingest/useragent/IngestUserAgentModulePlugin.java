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

package org.opensearch.ingest.useragent;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestUserAgentModulePlugin extends Plugin implements IngestPlugin {

    static final Setting<List<String>> PROCESSORS_ALLOWLIST_SETTING = Setting.listSetting(
        "ingest.useragent.processors.allowed",
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope
    );
    private final Setting<Long> CACHE_SIZE_SETTING = Setting.longSetting(
        "ingest.user_agent.cache_size",
        1000,
        0,
        Setting.Property.NodeScope
    );

    static final String DEFAULT_PARSER_NAME = "_default_";

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        Path userAgentConfigDirectory = parameters.env.configDir().resolve("ingest-user-agent");

        if (Files.exists(userAgentConfigDirectory) == false && Files.isDirectory(userAgentConfigDirectory)) {
            throw new IllegalStateException(
                "the user agent directory [" + userAgentConfigDirectory + "] containing the regex file doesn't exist"
            );
        }

        long cacheSize = CACHE_SIZE_SETTING.get(parameters.env.settings());
        Map<String, UserAgentParser> userAgentParsers;
        try {
            userAgentParsers = createUserAgentParsers(userAgentConfigDirectory, new UserAgentCache(cacheSize));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return filterForAllowlistSetting(
            parameters.env.settings(),
            Collections.singletonMap(UserAgentProcessor.TYPE, new UserAgentProcessor.Factory(userAgentParsers))
        );
    }

    private Map<String, Processor.Factory> filterForAllowlistSetting(Settings settings, Map<String, Processor.Factory> map) {
        if (PROCESSORS_ALLOWLIST_SETTING.exists(settings) == false) {
            return Map.copyOf(map);
        }
        final Set<String> allowlist = Set.copyOf(PROCESSORS_ALLOWLIST_SETTING.get(settings));
        // Assert that no unknown processors are defined in the allowlist
        final Set<String> unknownAllowlistProcessors = allowlist.stream()
            .filter(p -> map.containsKey(p) == false)
            .collect(Collectors.toUnmodifiableSet());
        if (unknownAllowlistProcessors.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Processor(s) "
                    + unknownAllowlistProcessors
                    + " were defined in ["
                    + PROCESSORS_ALLOWLIST_SETTING.getKey()
                    + "] but do not exist"
            );
        }
        return map.entrySet()
            .stream()
            .filter(e -> allowlist.contains(e.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static Map<String, UserAgentParser> createUserAgentParsers(Path userAgentConfigDirectory, UserAgentCache cache) throws IOException {
        Map<String, UserAgentParser> userAgentParsers = new HashMap<>();

        UserAgentParser defaultParser = new UserAgentParser(
            DEFAULT_PARSER_NAME,
            IngestUserAgentModulePlugin.class.getResourceAsStream("/regexes.yml"),
            cache
        );
        userAgentParsers.put(DEFAULT_PARSER_NAME, defaultParser);

        if (Files.exists(userAgentConfigDirectory) && Files.isDirectory(userAgentConfigDirectory)) {
            PathMatcher pathMatcher = userAgentConfigDirectory.getFileSystem().getPathMatcher("glob:**.yml");

            try (
                Stream<Path> regexFiles = Files.find(
                    userAgentConfigDirectory,
                    1,
                    (path, attr) -> attr.isRegularFile() && pathMatcher.matches(path)
                )
            ) {
                Iterable<Path> iterable = regexFiles::iterator;
                for (Path path : iterable) {
                    String parserName = path.getFileName().toString();
                    try (InputStream regexStream = Files.newInputStream(path, StandardOpenOption.READ)) {
                        userAgentParsers.put(parserName, new UserAgentParser(parserName, regexStream, cache));
                    }
                }
            }
        }

        return Collections.unmodifiableMap(userAgentParsers);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(CACHE_SIZE_SETTING);
    }
}
