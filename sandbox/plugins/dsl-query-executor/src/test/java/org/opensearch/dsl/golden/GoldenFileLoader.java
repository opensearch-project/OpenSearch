/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.dsl.executor.QueryPlans;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads and validates golden file test cases.
 *
 * <p>Each golden file is a self-contained JSON document parsed into a
 * {@link GoldenTestCase}. Required fields are validated after parsing;
 * aggregation test cases must additionally include {@code aggregationMetadata}.
 */
public class GoldenFileLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String RESOURCE_DIR = "golden/";

    private GoldenFileLoader() {}

    /**
     * Loads a golden file by name from the classpath resource directory
     * {@code src/test/resources/golden/}.
     *
     * @param goldenFileName file name (e.g. {@code "term_query_hits.json"})
     * @return parsed and validated test case
     * @throws IllegalArgumentException if the file is missing, malformed, or
     *         has missing required fields
     */
    public static GoldenTestCase load(String goldenFileName) {
        String resourcePath = RESOURCE_DIR + goldenFileName;
        try (InputStream is = GoldenFileLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Golden file not found on classpath: " + resourcePath);
            }
            GoldenTestCase testCase = MAPPER.readValue(is, GoldenTestCase.class);
            validate(testCase, Path.of(resourcePath));
            return testCase;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse golden file: " + resourcePath, e);
        }
    }

    /**
     * Loads a golden file from an absolute or relative file-system path.
     *
     * @param goldenFilePath path to the JSON golden file
     * @return parsed and validated test case
     * @throws IllegalArgumentException if the file is malformed or has missing
     *         required fields
     */
    public static GoldenTestCase load(Path goldenFilePath) {
        try (InputStream is = Files.newInputStream(goldenFilePath)) {
            GoldenTestCase testCase = MAPPER.readValue(is, GoldenTestCase.class);
            validate(testCase, goldenFilePath);
            return testCase;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse golden file: " + goldenFilePath, e);
        }
    }

    /**
     * Validates that all required fields are present in the parsed test case.
     * Throws {@link IllegalArgumentException} identifying the file and the
     * missing field.
     */
    private static void validate(GoldenTestCase testCase, Path filePath) {
        requireNonNull(testCase.getTestName(), "testName", filePath);
        requireNonNull(testCase.getIndexName(), "indexName", filePath);
        requireNonNull(testCase.getIndexMapping(), "indexMapping", filePath);
        requireNonNull(testCase.getInputDsl(), "inputDsl", filePath);
        requireNonNull(testCase.getExpectedRelNodePlan(), "expectedRelNodePlan", filePath);
        requireNonNull(testCase.getMockResultFieldNames(), "mockResultFieldNames", filePath);
        requireNonNull(testCase.getMockResultRows(), "mockResultRows", filePath);
        // Enforce explicit opt-out: a fixture must either supply expectedOutputDsl for response
        // assertions OR deliberately set planShapeOnly=true to indicate the multi-plan limitation
        // prevents response testing. Silent omission (missing both) is rejected to prevent
        // accidental loss of test coverage.
        if (testCase.getExpectedOutputDsl() == null && !testCase.isPlanShapeOnly()) {
            throw new IllegalArgumentException(
                "Golden file "
                    + filePath
                    + " is missing 'expectedOutputDsl' without setting 'planShapeOnly: true'. "
                    + "Either supply expectedOutputDsl for response assertions, or set "
                    + "\"planShapeOnly\": true to indicate this fixture intentionally tests plan shape only."
            );
        }
        if (testCase.getExpectedOutputDsl() != null && testCase.isPlanShapeOnly()) {
            throw new IllegalArgumentException(
                "Golden file "
                    + filePath
                    + " sets 'planShapeOnly: true' but also provides 'expectedOutputDsl'. "
                    + "This is contradictory — remove expectedOutputDsl or set planShapeOnly to false."
            );
        }
        requireNonNull(testCase.getPlanType(), "planType", filePath);
        try {
            QueryPlans.Type.valueOf(testCase.getPlanType());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Golden file " + filePath + " has invalid planType: " + testCase.getPlanType());
        }
    }

    private static void requireNonNull(Object value, String fieldName, Path filePath) {
        if (value == null) {
            throw new IllegalArgumentException("Golden file " + filePath + " missing required field: " + fieldName);
        }
    }
}
