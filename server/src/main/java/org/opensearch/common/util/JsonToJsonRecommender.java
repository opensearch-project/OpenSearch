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

package org.opensearch.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for analyzing and mapping relationships between input and output JSON structures.
 *
 * Builds an inverted index from input JSON values to their paths, then uses output JSON values
 * to find matching input paths, producing a mapping from input paths to output fields.
 *
 * Supports nested objects and arrays, and generates both detailed field mappings and generalized
 * JSONPath transformation patterns.
 *
 * All methods are static and stateless. This class is thread-safe.
 *
 * @opensearch.internal
 */
public class JsonToJsonRecommender {

    // ========================================================================================
    // Static Variables
    // ========================================================================================

    /**
     * ObjectMapper instance used for parsing JSON strings and formatting output.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ========================================================================================
    // Public Methods
    // ========================================================================================

    /**
     * Generates mapping data with default settings (detailed output is empty).
     * This is a convenience method that calls {@link #getRecommendation(String, String, boolean)}
     * with showDetails set to false.
     *
     * @param inputJson   Input JSON string to analyze
     * @param outputJson  Output JSON string to map against
     * @return MappingOutput containing transformation recommendations
     * @throws Exception if JSON parsing fails or processing encounters errors
     */
    public static MappingOutput getRecommendation(String inputJson, String outputJson) throws Exception {
        return getRecommendation(inputJson, outputJson, false);
    }

    /**
     * Generates mapping data and optionally includes formatted visual output.
     * This is the main entry point for getting transformation recommendations
     * from input and output JSON structures.
     *
     * @param inputJson   Input JSON string to analyze
     * @param outputJson  Output JSON string to map against
     * @param showDetails If true, includes formatted detailed output in the verbose field;
     *                    otherwise verbose field will be empty
     * @return MappingOutput containing:
     *         - detailedJsonPathString: field-to-field mapping as JSON string
     *         - generalizedJsonPathString: suggested JsonPaths as JSON string
     *         - verbose: formatted output when showDetails is true, empty otherwise
     * @throws Exception if JSON parsing fails or processing encounters errors
     */
    public static MappingOutput getRecommendation(String inputJson, String outputJson, boolean showDetails) throws Exception {
        if (inputJson == null || outputJson == null) {
            throw new IllegalArgumentException("Input and output JSON strings cannot be null");
        }

        JsonNode inputNode = MAPPER.readTree(inputJson);
        JsonNode outputNode = MAPPER.readTree(outputJson);

        Map<String, String> inputIndex = createInvertedIndex(inputNode);

        MappingResult result = mapStructures(inputIndex, outputNode);

        String detailedJsonPathString = MAPPER.writerWithDefaultPrettyPrinter()
            .writeValueAsString(toNestedMapping(result.detailedJsonPath));
        String generalizedJsonPathString = MAPPER.writerWithDefaultPrettyPrinter()
            .writeValueAsString(toNestedMapping(result.generalizedJsonPath));

        String verbose = "";
        if (showDetails) {
            StringBuilder visualizationString = new StringBuilder();
            visualizationString.append("Detailed Field Mapping:\n");
            visualizationString.append(formatJsonPath(result.detailedJsonPath));
            visualizationString.append("\n\n");
            visualizationString.append("Generalized Field Mapping:\n");
            visualizationString.append(formatJsonPath(result.generalizedJsonPath));
            verbose = visualizationString.toString();
        }

        return new MappingOutput(detailedJsonPathString, generalizedJsonPathString, verbose);
    }

    /**
     * Builds an inverted index for a given JSON node.
     * Each unique value is mapped to a single JSON path where it appears.
     * For duplicate values, only the first occurrence path is stored.
     *
     * @param node The JSON node to process
     * @return A map of values to their JSON paths
     * @throws IllegalArgumentException if the node is null
     */
    public static Map<String, String> createInvertedIndex(JsonNode node) {
        if (node == null) {
            throw new IllegalArgumentException("JSON node cannot be null");
        }

        Map<String, String> index = new HashMap<>();
        createIndexRecursive(node, "$", index);
        return index;
    }

    /**
     * Generates output-to-input mappings by recursively traversing the output JSON
     * node and correlating values with the input index. Creates both detailed and
     * generalized mappings, with special handling for arrays to provide useful transformation
     * patterns.
     *
     * @param outputNode The output JSON node to traverse
     * @param inputIndex The inverted index of input JSON values to paths
     * @param path       The current JSON path being processed (typically starts
     *                   with "$")
     * @return MappingResult containing detailed and generalized mappings
     * @throws IllegalArgumentException if any parameter is null
     */
    public static MappingResult generateMappings(JsonNode outputNode, Map<String, String> inputIndex, String path) {
        if (outputNode == null || inputIndex == null || path == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }

        Map<String, String> detailed = new LinkedHashMap<>();
        Map<String, String> generalized = new LinkedHashMap<>();

        generateMappingsRecursive(outputNode, inputIndex, path, detailed, generalized);

        return new MappingResult(detailed, generalized);
    }

    /**
     * Maps values from the output JSON node to the corresponding input index.
     * Generates both detailed field mappings and generalized JSONPath suggestions
     * using recursive mapping generation approach with intelligent array handling.
     *
     * @param inputIndex The inverted index of the input JSON
     * @param outputNode The output JSON node to process
     * @return A MappingResult containing detailed mappings and JSONPath
     *         suggestions
     * @throws IllegalArgumentException if any parameter is null
     */
    public static MappingResult mapStructures(Map<String, String> inputIndex, JsonNode outputNode) {
        if (inputIndex == null || outputNode == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }

        // Use the generateMappings method to create mappings
        MappingResult result = generateMappings(outputNode, inputIndex, "$");

        // Return the result with detailed and generalized mappings
        return new MappingResult(result.detailedJsonPath, result.generalizedJsonPath);
    }

    /**
     * Formats the detailed mapping in a tabular format.
     * Creates a well-formatted table showing the relationship between output and
     * input fields.
     *
     * @param mapping The detailed mapping to format
     * @return A formatted string representation of the mapping table
     * @throws IllegalArgumentException if mapping is null
     */
    public static String formatJsonPath(Map<String, String> mapping) {
        if (mapping == null) {
            throw new IllegalArgumentException("Mapping cannot be null");
        }

        int maxOut = Math.max("Output Field".length(), mapping.keySet().stream().mapToInt(String::length).max().orElse(12));
        int maxIn = Math.max("Input Field".length(), mapping.values().stream().mapToInt(String::length).max().orElse(11));

        String line = "-".repeat(maxOut + maxIn + 7);
        StringBuilder sb = new StringBuilder(line).append('\n');
        sb.append(String.format(Locale.ROOT, "| %-" + maxOut + "s | %-" + maxIn + "s |%n", "Output Field", "Input Field"))
            .append(line)
            .append('\n');
        mapping.forEach((k, v) -> sb.append(String.format(Locale.ROOT, "| %-" + maxOut + "s | %-" + maxIn + "s |%n", k, v)));
        sb.append(line);
        return sb.toString();
    }

    /**
     * Converts a flat Map with dot-separated paths into a nested Map structure.
     * This transformation makes it easier to understand the hierarchical
     * relationship of the mappings and can be used for generating configuration files or
     * templates.
     *
     * <p>
     * Example transformation:
     * Input: "allocDetails[*].team.product.id" =>
     * "$.item.allocDetails.items[*].team.product.id"
     * Output: Nested map structure representing the hierarchy
     *
     * @param flatMap The flat mapping with dot-separated keys
     * @return A nested Map representing the hierarchical structure
     * @throws IllegalArgumentException if flatMap is null
     * @throws IllegalStateException    if there are conflicting key structures
     */
    public static Map<String, Object> toNestedMapping(Map<String, String> flatMap) {
        if (flatMap == null) {
            throw new IllegalArgumentException("Flat map cannot be null");
        }

        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : flatMap.entrySet()) {
            String keyPath = entry.getKey();

            // Remove leading "$." if present for cleaner processing
            if (keyPath.startsWith("$.")) {
                keyPath = keyPath.substring(2);
            }

            String[] parts = keyPath.split("\\.");

            // Filter out empty parts to handle edge cases
            List<String> validParts = new ArrayList<>();
            for (String part : parts) {
                if (!part.isEmpty()) {
                    validParts.add(part);
                }
            }

            Map<String, Object> current = result;

            // Build nested structure
            for (int i = 0; i < validParts.size(); i++) {
                String key = validParts.get(i);
                boolean isLast = (i == validParts.size() - 1);

                if (isLast) {
                    current.put(key, entry.getValue());
                } else {
                    current = getOrCreateChildMap(current, key);
                }
            }
        }

        return result;
    }

    // ========================================================================================
    // Private Methods
    // ========================================================================================

    /**
     * Recursively traverses the JSON node to build the inverted index.
     * This method handles objects, arrays, and primitive values differently to
     * create comprehensive path mappings.
     *
     * @param node  The current JSON node being processed
     * @param path  The JSON path leading to this node
     * @param index The index to populate with value-to-path mappings
     */
    private static void createIndexRecursive(JsonNode node, String path, Map<String, String> index) {
        if (node.isObject()) {
            // Process object properties recursively
            node.fields().forEachRemaining(e -> {
                String newPath = path.isEmpty() ? e.getKey() : path + "." + e.getKey();
                createIndexRecursive(e.getValue(), newPath, index);
            });
        } else if (node.isArray()) {
            // Process each array element with indexed path
            for (int i = 0; i < node.size(); i++) {
                createIndexRecursive(node.get(i), path + "[" + i + "]", index);
            }
        } else {
            // Store primitive values with their paths
            String value = node.asText();
            index.putIfAbsent(value, path); // Only store the first occurrence of a value
        }
    }

    /**
     * Recursively processes JSON nodes to build output-to-input mappings.
     * Handles special array processing logic where array indices are generalized
     * when elements have similar structures.
     *
     * @param node        The current JSON node being processed
     * @param inputIndex  The inverted index of input JSON values to paths
     * @param path        The current JSON path
     * @param detailed    The detailed mapping accumulator
     * @param generalized The generalized mapping accumulator
     */
    private static void generateMappingsRecursive(
        JsonNode node,
        Map<String, String> inputIndex,
        String path,
        Map<String, String> detailed,
        Map<String, String> generalized
    ) {
        if (node.isObject()) {
            // Process object nodes recursively
            node.fields().forEachRemaining(entry -> {
                String newPath = path.equals("$") ? "$." + entry.getKey() : path + "." + entry.getKey();
                generateMappingsRecursive(entry.getValue(), inputIndex, newPath, detailed, generalized);
            });

        } else if (node.isArray()) {
            // Process array nodes with special logic for generalization
            List<Map<String, String>> arrayDetailedMappings = new ArrayList<>();
            List<Map<String, String>> arrayGeneralizedMappings = new ArrayList<>();

            // Collect mappings for each array element
            for (int i = 0; i < node.size(); i++) {
                String arrayElementPath = path + "[" + i + "]";
                Map<String, String> elementDetailed = new LinkedHashMap<>();
                Map<String, String> elementGeneralized = new LinkedHashMap<>();

                generateMappingsRecursive(node.get(i), inputIndex, arrayElementPath, elementDetailed, elementGeneralized);

                arrayDetailedMappings.add(elementDetailed);
                arrayGeneralizedMappings.add(elementGeneralized);
            }

            // Check if array elements have similar structure (only array indices differ)
            if (arrayGeneralizedMappings.size() > 1 && areArrayMappingsSimilar(arrayGeneralizedMappings)) {
                // Add all detailed mappings
                for (Map<String, String> elementMapping : arrayDetailedMappings) {
                    detailed.putAll(elementMapping);
                }

                // Add generalized mapping with smart array index replacement
                Map<String, String> firstElementGeneralized = arrayGeneralizedMappings.get(0);
                for (Map.Entry<String, String> entry : firstElementGeneralized.entrySet()) {
                    // Collect all corresponding keys for this entry across array elements
                    Set<String> allKeys = new HashSet<>();
                    Set<String> allValues = new HashSet<>();

                    for (Map<String, String> mapping : arrayGeneralizedMappings) {
                        for (Map.Entry<String, String> e : mapping.entrySet()) {
                            if (e.getKey().replaceAll("\\[\\d+\\]", "[*]").equals(entry.getKey().replaceAll("\\[\\d+\\]", "[*]"))) {
                                allKeys.add(e.getKey());
                                allValues.add(e.getValue());
                            }
                        }
                    }

                    String generalizedKey = generalizeVaryingArrayIndices(allKeys);
                    String generalizedValue = generalizeVaryingArrayIndices(allValues);
                    generalized.put(generalizedKey, generalizedValue);
                }
            } else {
                // Array elements have different structures, add all mappings to both detailed
                // and generalized
                for (int i = 0; i < arrayDetailedMappings.size(); i++) {
                    detailed.putAll(arrayDetailedMappings.get(i));
                    generalized.putAll(arrayGeneralizedMappings.get(i));
                }
            }

        } else {
            // Process leaf values by finding matching input paths
            String value = node.asText();
            String matchingInputPath = inputIndex.get(value);

            if (matchingInputPath != null && !matchingInputPath.isEmpty()) {
                detailed.put(path, matchingInputPath);
                generalized.put(path, matchingInputPath);
            }
        }
    }

    /**
     * Checks if array mappings are similar (only differing in array indices).
     * This determines whether to use generalized array notation or treat each
     * element separately, which is crucial for creating useful transformation
     * patterns.
     *
     * @param arrayMappings List of generalized mappings for array elements
     * @return true if mappings have similar structure with only index differences
     */
    private static boolean areArrayMappingsSimilar(List<Map<String, String>> arrayMappings) {
        if (arrayMappings.size() <= 1) return true;

        Map<String, String> firstMapping = arrayMappings.get(0);
        Set<String> firstKeys = firstMapping.keySet().stream().map(key -> key.replaceAll("\\[\\d+\\]", "[*]")).collect(Collectors.toSet());

        for (int i = 1; i < arrayMappings.size(); i++) {
            Map<String, String> currentMapping = arrayMappings.get(i);
            Set<String> currentKeys = currentMapping.keySet()
                .stream()
                .map(key -> key.replaceAll("\\[\\d+\\]", "[*]"))
                .collect(Collectors.toSet());

            if (!firstKeys.equals(currentKeys)) {
                return false;
            }

            // Check if the values also have similar structure when generalized
            for (String generalizedKey : firstKeys) {
                String firstValue = firstMapping.entrySet()
                    .stream()
                    .filter(e -> e.getKey().replaceAll("\\[\\d+\\]", "[*]").equals(generalizedKey))
                    .map(e -> e.getValue().replaceAll("\\[\\d+\\]", "[*]"))
                    .findFirst()
                    .orElse("");

                String currentValue = currentMapping.entrySet()
                    .stream()
                    .filter(e -> e.getKey().replaceAll("\\[\\d+\\]", "[*]").equals(generalizedKey))
                    .map(e -> e.getValue().replaceAll("\\[\\d+\\]", "[*]"))
                    .findFirst()
                    .orElse("");

                if (!firstValue.equals(currentValue)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Generalizes array indices by replacing only varying indices with [*].
     * This method intelligently identifies which array indices are varying
     * across similar paths and generalizes only those positions.
     *
     * @param paths Set of paths to analyze and generalize
     * @return Generalized path with [*] only for varying array indices
     */
    private static String generalizeVaryingArrayIndices(Set<String> paths) {
        if (paths.isEmpty()) return "";
        if (paths.size() == 1) return paths.iterator().next();

        List<String[]> split = paths.stream().map(p -> p.split("(?<=]\\.)|(?=\\[)|\\.")).collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();
        int max = split.stream().mapToInt(a -> a.length).max().orElse(0);

        for (int i = 0; i < max; i++) {
            int index = i;
            Set<String> parts = split.stream().map(arr -> index < arr.length ? arr[index] : "").collect(Collectors.toSet());

            // Add separator if needed
            if (i > 0 && sb.length() > 0 && !sb.toString().endsWith(".") && !parts.iterator().next().startsWith("[")) {
                sb.append('.');
            }

            if (parts.size() == 1) {
                // All paths have the same part at this position, keep it unchanged
                sb.append(parts.iterator().next());
            } else if (parts.stream().anyMatch(p -> p.matches("\\[\\d+]"))) {
                // Array indices vary at this position, replace with [*]
                sb.append("[*]");
            } else {
                // Other variations, keep the first one
                sb.append(parts.iterator().next());
            }
        }
        return sb.toString();
    }

    /**
     * Safely retrieves or creates a nested <code>Map&lt;String, Object&gt;</code>
     * under a given key.
     * This method ensures type safety when building nested structures and provides
     * clear error messages when there are structural conflicts.
     *
     * @param parent The parent map to get or create the child map in
     * @param key    The key under which to get or create the child map
     * @return The existing or newly created child map
     * @throws IllegalStateException if there's a type conflict at the specified key
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getOrCreateChildMap(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        if (value == null) {
            Map<String, Object> child = new LinkedHashMap<>();
            parent.put(key, child);
            return child;
        } else if (value instanceof Map) {
            return (Map<String, Object>) value;
        } else {
            throw new IllegalStateException(
                "Conflict at key '" + key + "': expected a nested object but found " + value.getClass().getSimpleName()
            );
        }
    }

    // ========================================================================================
    // Inner Classes
    // ========================================================================================

    /**
     * Record representing the mapping result, containing detailed mappings and
     * JSONPath suggestions. This provides both granular field-to-field mappings
     * and generalized transformation patterns.
     *
     * @param detailedJsonPath    Detailed field-to-field mappings with input paths
     * @param generalizedJsonPath Simplified JSONPath transformation suggestions
     */
    public record MappingResult(Map<String, String> detailedJsonPath, Map<String, String> generalizedJsonPath) {
    }

    /**
     * Container class for the final mapping output in string format.
     * Provides the transformation recommendations as formatted JSON strings
     * that can be easily consumed by other systems or displayed to users.
     */
    public static class MappingOutput {
        /** Detailed mapping as a formatted JSON string */
        public final String detailedJsonPathString;

        /** Generalized JSONPath suggestions as a formatted JSON string */
        public final String generalizedJsonPathString;

        /** Verbose information for detailed analysis */
        public final String verbose;

        /**
         * Constructs a new MappingOutput with the provided mapping strings.
         *
         * @param detailedJsonPathString    The detailed mapping as JSON string
         * @param generalizedJsonPathString The generalized mapping as JSON string
         * @param verbose                   The verbose information string
         */
        public MappingOutput(String detailedJsonPathString, String generalizedJsonPathString, String verbose) {
            this.detailedJsonPathString = detailedJsonPathString;
            this.generalizedJsonPathString = generalizedJsonPathString;
            this.verbose = verbose;
        }
    }
}
