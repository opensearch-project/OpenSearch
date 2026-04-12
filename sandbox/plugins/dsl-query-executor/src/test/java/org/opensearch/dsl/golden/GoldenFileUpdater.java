/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handles golden file update mode for regenerating expected outputs.
 *
 * <p>When the system property {@code golden.update=true} is set, the test
 * runner calls {@link #update} instead of asserting. This overwrites the
 * {@code expectedRelNodePlan} and {@code expectedOutputDsl} fields in the
 * golden file while preserving all input fields.
 */
public class GoldenFileUpdater {

    private static final Logger logger = LogManager.getLogger(GoldenFileUpdater.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final String UPDATE_PROPERTY = "golden.update";

    private GoldenFileUpdater() {}

    /**
     * Returns {@code true} if the system property {@code golden.update} is
     * set to {@code "true"}.
     */
    public static boolean isUpdateMode() {
        return "true".equals(System.getProperty(UPDATE_PROPERTY));
    }

    /**
     * Overwrites the expected fields in the golden file with actual computed
     * values while preserving all input fields.
     *
     * @param goldenFilePath      path to the golden JSON file on disk
     * @param actualRelNodePlan   the actual RelNode.explain() output
     * @param actualOutputDsl     the actual SearchResponse JSON as a map
     * @throws IOException if the file cannot be read or written
     */
    public static void update(Path goldenFilePath, String actualRelNodePlan, Map<String, Object> actualOutputDsl)
        throws IOException {
        Map<String, Object> content = MAPPER.readValue(
            Files.readString(goldenFilePath),
            new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        content.put("expectedRelNodePlan", actualRelNodePlan);
        content.put("expectedOutputDsl", actualOutputDsl);

        MAPPER.writeValue(goldenFilePath.toFile(), content);

        logger.warn("GOLDEN FILE UPDATED: {} — review the diff before committing", goldenFilePath);
    }

    /**
     * Overwrites only the {@code expectedRelNodePlan} field in the golden file,
     * leaving {@code expectedOutputDsl} unchanged. Used by the forward path
     * update mode to avoid writing stale output DSL data.
     *
     * @param goldenFilePath      path to the golden JSON file on disk
     * @param actualRelNodePlan   the actual RelNode.explain() output
     * @throws IOException if the file cannot be read or written
     */
    public static void updatePlan(Path goldenFilePath, String actualRelNodePlan) throws IOException {
        Map<String, Object> content = MAPPER.readValue(
            Files.readString(goldenFilePath),
            new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        content.put("expectedRelNodePlan", actualRelNodePlan);

        MAPPER.writeValue(goldenFilePath.toFile(), content);

        logger.warn("GOLDEN FILE PLAN UPDATED: {} — review the diff before committing", goldenFilePath);
    }
}
