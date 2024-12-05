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

package org.opensearch.grok;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.joni.Syntax;
import org.joni.exception.ValueException;

import static java.util.Collections.unmodifiableList;

public final class Grok {
    /**
     * Patterns built in to the grok library.
     */
    public static final Map<String, String> BUILTIN_PATTERNS = loadBuiltinPatterns();

    private static final String NAME_GROUP = "name";
    private static final String SUBNAME_GROUP = "subname";
    private static final String PATTERN_GROUP = "pattern";
    private static final String DEFINITION_GROUP = "definition";
    private static final String GROK_PATTERN = "%\\{"
        + "(?<name>"
        + "(?<pattern>[A-z0-9]+)"
        + "(?::(?<subname>[[:alnum:]@\\[\\]_:.-]+))?"
        + ")"
        + "(?:=(?<definition>"
        + "(?:[^{}]+|\\.+)+"
        + ")"
        + ")?"
        + "\\}";
    private static final Regex GROK_PATTERN_REGEX = new Regex(
        GROK_PATTERN.getBytes(StandardCharsets.UTF_8),
        0,
        GROK_PATTERN.getBytes(StandardCharsets.UTF_8).length,
        Option.NONE,
        UTF8Encoding.INSTANCE,
        Syntax.DEFAULT
    );
    private static final int MAX_PATTERN_DEPTH_SIZE = 500;

    private static final int MAX_TO_REGEX_ITERATIONS = 100_000; // sanity limit

    private final Map<String, String> patternBank;
    private final boolean namedCaptures;
    private final Regex compiledExpression;
    private final MatcherWatchdog matcherWatchdog;
    private final List<GrokCaptureConfig> captureConfig;

    public Grok(Map<String, String> patternBank, String grokPattern, Consumer<String> logCallBack) {
        this(patternBank, grokPattern, true, MatcherWatchdog.noop(), logCallBack);
    }

    public Grok(Map<String, String> patternBank, String grokPattern, MatcherWatchdog matcherWatchdog, Consumer<String> logCallBack) {
        this(patternBank, grokPattern, true, matcherWatchdog, logCallBack);
    }

    Grok(Map<String, String> patternBank, String grokPattern, boolean namedCaptures, Consumer<String> logCallBack) {
        this(patternBank, grokPattern, namedCaptures, MatcherWatchdog.noop(), logCallBack);
    }

    private Grok(
        Map<String, String> patternBank,
        String grokPattern,
        boolean namedCaptures,
        MatcherWatchdog matcherWatchdog,
        Consumer<String> logCallBack
    ) {
        this.patternBank = patternBank;
        this.namedCaptures = namedCaptures;
        this.matcherWatchdog = matcherWatchdog;

        validatePatternBank();

        String expression = toRegex(grokPattern);
        byte[] expressionBytes = expression.getBytes(StandardCharsets.UTF_8);
        this.compiledExpression = new Regex(
            expressionBytes,
            0,
            expressionBytes.length,
            Option.DEFAULT,
            UTF8Encoding.INSTANCE,
            logCallBack::accept
        );

        List<GrokCaptureConfig> captureConfig = new ArrayList<>();
        for (Iterator<NameEntry> entry = compiledExpression.namedBackrefIterator(); entry.hasNext();) {
            captureConfig.add(new GrokCaptureConfig(entry.next()));
        }
        this.captureConfig = unmodifiableList(captureConfig);
    }

    /**
     * Entry point to recursively validate the pattern bank for circular dependencies and malformed URLs
     * via depth-first traversal. This implementation does not include memoization.
     */
    private void validatePatternBank() {
        for (String patternName : patternBank.keySet()) {
            validatePatternBank(patternName);
        }
    }

    /**
     * Checks whether patterns reference each other in a circular manner and, if so, fail with an exception.
     * Also checks for malformed pattern definitions and fails with an exception.
     * <p>
     * In a pattern, anything between <code>%{</code> and <code>}</code> or <code>:</code> is considered
     * a reference to another named pattern. This method will navigate to all these named patterns and
     * check for a circular reference.
     */
    private void validatePatternBank(String initialPatternName) {
        Deque<Frame> stack = new ArrayDeque<>();
        Set<String> visitedPatterns = new HashSet<>();
        Map<String, List<String>> pathMap = new HashMap<>();

        List<String> initialPath = new ArrayList<>();
        initialPath.add(initialPatternName);
        pathMap.put(initialPatternName, initialPath);
        stack.push(new Frame(initialPatternName, initialPath, 0));

        while (!stack.isEmpty()) {
            Frame frame = stack.peek();
            String patternName = frame.patternName;
            List<String> path = frame.path;
            int startIndex = frame.startIndex;
            String pattern = patternBank.get(patternName);

            if (visitedPatterns.contains(patternName)) {
                stack.pop();
                continue;
            }

            visitedPatterns.add(patternName);
            boolean foundDependency = false;

            for (int i = startIndex; i < pattern.length(); i++) {
                if (pattern.startsWith("%{", i)) {
                    int begin = i + 2;
                    int syntaxEndIndex = pattern.indexOf('}', begin);
                    if (syntaxEndIndex == -1) {
                        throw new IllegalArgumentException("Malformed pattern [" + patternName + "][" + pattern + "]");
                    }

                    int semanticNameIndex = pattern.indexOf(':', begin);
                    int end = semanticNameIndex == -1 ? syntaxEndIndex : Math.min(syntaxEndIndex, semanticNameIndex);

                    String dependsOnPattern = pattern.substring(begin, end);

                    if (dependsOnPattern.equals(patternName)) {
                        throwExceptionForCircularReference(patternName, pattern);
                    }

                    if (pathMap.containsKey(dependsOnPattern)) {
                        throwExceptionForCircularReference(patternName, pattern, dependsOnPattern, path.subList(0, path.size() - 1));
                    }

                    List<String> newPath = new ArrayList<>(path);
                    newPath.add(dependsOnPattern);
                    pathMap.put(dependsOnPattern, newPath);

                    stack.push(new Frame(dependsOnPattern, newPath, 0));
                    frame.startIndex = i + 1;
                    foundDependency = true;
                    break;
                }
            }

            if (!foundDependency) {
                pathMap.remove(patternName);
                stack.pop();
            }

            if (stack.size() > MAX_PATTERN_DEPTH_SIZE) {
                throw new IllegalArgumentException("Pattern references exceeded maximum depth of " + MAX_PATTERN_DEPTH_SIZE);
            }
        }
    }

    private static class Frame {
        String patternName;
        List<String> path;
        int startIndex;

        Frame(String patternName, List<String> path, int startIndex) {
            this.patternName = patternName;
            this.path = path;
            this.startIndex = startIndex;
        }
    }

    private static void throwExceptionForCircularReference(String patternName, String pattern) {
        throwExceptionForCircularReference(patternName, pattern, null, null);
    }

    private static void throwExceptionForCircularReference(
        String patternName,
        String pattern,
        String originPatternName,
        List<String> path
    ) {
        StringBuilder message = new StringBuilder("circular reference in pattern [");
        message.append(patternName).append("][").append(pattern).append("]");
        if (originPatternName != null) {
            message.append(" back to pattern [").append(originPatternName).append("]");
        }
        if (path != null && path.size() > 1) {
            message.append(" via patterns [").append(String.join("=>", path)).append("]");
        }
        throw new IllegalArgumentException(message.toString());
    }

    private String groupMatch(String name, Region region, String pattern) {
        try {
            int number = GROK_PATTERN_REGEX.nameToBackrefNumber(
                name.getBytes(StandardCharsets.UTF_8),
                0,
                name.getBytes(StandardCharsets.UTF_8).length,
                region
            );
            int begin = region.getBeg(number);
            int end = region.getEnd(number);
            return new String(pattern.getBytes(StandardCharsets.UTF_8), begin, end - begin, StandardCharsets.UTF_8);
        } catch (StringIndexOutOfBoundsException | ValueException e) {
            return null;
        }
    }

    /**
     * converts a grok expression into a named regex expression
     *
     * @return named regex expression
     */
    protected String toRegex(String grokPattern) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < MAX_TO_REGEX_ITERATIONS; i++) {
            byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
            Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

            int result;
            try {
                matcherWatchdog.register(matcher);
                result = matcher.search(0, grokPatternBytes.length, Option.NONE);
            } finally {
                matcherWatchdog.unregister(matcher);
            }

            if (result < 0) {
                return res.append(grokPattern).toString();
            }

            Region region = matcher.getEagerRegion();
            String namedPatternRef = groupMatch(NAME_GROUP, region, grokPattern);
            String subName = groupMatch(SUBNAME_GROUP, region, grokPattern);
            // TODO(tal): Support definitions
            @SuppressWarnings("unused")
            String definition = groupMatch(DEFINITION_GROUP, region, grokPattern);
            String patternName = groupMatch(PATTERN_GROUP, region, grokPattern);
            String pattern = patternBank.get(patternName);
            if (pattern == null) {
                throw new IllegalArgumentException("Unable to find pattern [" + patternName + "] in Grok's pattern dictionary");
            }
            if (pattern.contains("%{" + patternName + "}") || pattern.contains("%{" + patternName + ":")) {
                throw new IllegalArgumentException("circular reference in pattern back [" + patternName + "]");
            }
            String grokPart;
            if (namedCaptures && subName != null) {
                grokPart = String.format(Locale.US, "(?<%s>%s)", namedPatternRef, pattern);
            } else if (namedCaptures) {
                grokPart = String.format(Locale.US, "(?:%s)", pattern);
            } else {
                grokPart = String.format(Locale.US, "(?<%s>%s)", patternName + "_" + result, pattern);
            }
            String start = new String(grokPatternBytes, 0, result, StandardCharsets.UTF_8);
            String rest = new String(
                grokPatternBytes,
                region.getEnd(0),
                grokPatternBytes.length - region.getEnd(0),
                StandardCharsets.UTF_8
            );
            grokPattern = grokPart + rest;
            res.append(start);
        }
        throw new IllegalArgumentException("Can not convert grok patterns to regular expression");
    }

    /**
     * Checks whether a specific text matches the defined grok expression.
     *
     * @param text the string to match
     * @return true if grok expression matches text or there is a timeout, false otherwise.
     */
    public boolean match(String text) {
        Matcher matcher = compiledExpression.matcher(text.getBytes(StandardCharsets.UTF_8));
        int result;
        try {
            matcherWatchdog.register(matcher);
            result = matcher.search(0, text.length(), Option.DEFAULT);
        } finally {
            matcherWatchdog.unregister(matcher);
        }
        return (result != -1);
    }

    /**
     * Matches and returns any named captures.
     *
     * @param text the text to match and extract values from.
     * @return a map containing field names and their respective coerced values that matched or null if the pattern didn't match
     */
    public Map<String, Object> captures(String text) {
        byte[] utf8Bytes = text.getBytes(StandardCharsets.UTF_8);
        GrokCaptureExtracter.MapExtracter extracter = new GrokCaptureExtracter.MapExtracter(captureConfig);
        if (match(utf8Bytes, 0, utf8Bytes.length, extracter)) {
            return extracter.result();
        }
        return null;
    }

    /**
     * Matches and collects any named captures.
     * @param utf8Bytes array containing the text to match against encoded in utf-8
     * @param offset offset {@code utf8Bytes} of the start of the text
     * @param length length of the text to match
     * @param extracter collector for captures. {@link GrokCaptureConfig#nativeExtracter} can build these.
     * @return true if there was a match, false otherwise
     * @throws RuntimeException if there was a timeout
     */
    public boolean match(byte[] utf8Bytes, int offset, int length, GrokCaptureExtracter extracter) {
        Matcher matcher = compiledExpression.matcher(utf8Bytes, offset, offset + length);
        int result;
        try {
            matcherWatchdog.register(matcher);
            result = matcher.search(offset, length, Option.DEFAULT);
        } finally {
            matcherWatchdog.unregister(matcher);
        }
        if (result == Matcher.INTERRUPTED) {
            throw new RuntimeException(
                "grok pattern matching was interrupted after [" + matcherWatchdog.maxExecutionTimeInMillis() + "] ms"
            );
        }
        if (result == Matcher.FAILED) {
            return false;
        }
        extracter.extract(utf8Bytes, offset, matcher.getEagerRegion());
        return true;
    }

    /**
     * The list of values that this {@linkplain Grok} can capture.
     */
    public List<GrokCaptureConfig> captureConfig() {
        return captureConfig;
    }

    /**
     * Load built-in patterns.
     */
    private static Map<String, String> loadBuiltinPatterns() {
        String[] patternNames = new String[] {
            "aws",
            "bacula",
            "bind",
            "bro",
            "exim",
            "firewalls",
            "grok-patterns",
            "haproxy",
            "java",
            "junos",
            "linux-syslog",
            "maven",
            "mcollective-patterns",
            "mongodb",
            "nagios",
            "postgresql",
            "rails",
            "redis",
            "ruby",
            "squid" };
        Map<String, String> builtinPatterns = new LinkedHashMap<>();
        for (String pattern : patternNames) {
            try {
                try (InputStream is = Grok.class.getResourceAsStream("/patterns/" + pattern)) {
                    loadPatterns(builtinPatterns, is);
                }
            } catch (IOException e) {
                throw new RuntimeException("failed to load built-in patterns", e);
            }
        }
        return Collections.unmodifiableMap(builtinPatterns);
    }

    private static void loadPatterns(Map<String, String> patternBank, InputStream inputStream) throws IOException {
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        while ((line = br.readLine()) != null) {
            String trimmedLine = line.replaceAll("^\\s+", "");
            if (trimmedLine.startsWith("#") || trimmedLine.length() == 0) {
                continue;
            }

            String[] parts = trimmedLine.split("\\s+", 2);
            if (parts.length == 2) {
                patternBank.put(parts[0], parts[1]);
            }
        }
    }

}
