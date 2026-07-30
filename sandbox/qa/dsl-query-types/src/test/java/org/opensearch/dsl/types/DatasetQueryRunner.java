/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.RestClient;
import org.opensearch.common.io.PathUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Discovers DSL query files from a dataset's resource dir and runs them against a live cluster.
 * For a dataset at {@code resources/datasets/{name}/}, queries are auto-discovered from
 * {@code {language}/q{N}.{ext}} and executed via the supplied {@link QueryExecutor}. Failures are
 * collected (not fail-fast) so every query is attempted.
 *
 * <p>Local copy tailored to this standalone module (no dependency on analytics-engine-rest).
 */
public final class DatasetQueryRunner {

    private static final Logger logger = LogManager.getLogger(DatasetQueryRunner.class);
    private static final Pattern QUERY_FILE_PATTERN = Pattern.compile("q(\\d+)\\.\\w+");

    /** Executes a single query against a live cluster and returns the response body as a Map. */
    @FunctionalInterface
    public interface QueryExecutor {
        Map<String, Object> execute(RestClient client, Dataset dataset, String queryBody) throws IOException;
    }

    private DatasetQueryRunner() {}

    /**
     * Discover all query numbers for the given dataset/language — sorted list of N such that
     * {@code {language}/q{N}.{ext}} exists.
     */
    public static List<Integer> discoverQueryNumbers(Dataset dataset, String language) throws IOException {
        String resourceDir = "datasets/" + dataset.name + "/" + language;
        URL url = DatasetQueryRunner.class.getClassLoader().getResource(resourceDir);
        if (url == null) {
            return Collections.emptyList();
        }
        List<Integer> numbers = new ArrayList<>();
        FileSystem fs = null;
        try {
            URI uri = url.toURI();
            Path path;
            if ("jar".equals(uri.getScheme())) {
                fs = FileSystems.newFileSystem(uri, Collections.emptyMap());
                path = fs.getPath(resourceDir);
            } else {
                path = PathUtils.get(uri);
            }
            try (Stream<Path> stream = Files.list(path)) {
                stream.forEach(p -> {
                    Matcher m = QUERY_FILE_PATTERN.matcher(p.getFileName().toString());
                    if (m.matches()) {
                        numbers.add(Integer.parseInt(m.group(1)));
                    }
                });
            }
        } catch (Exception e) {
            throw new IOException("Failed to discover queries for dataset [" + dataset.name + "] language [" + language + "]", e);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        Collections.sort(numbers);
        return numbers;
    }

    /**
     * Run queries against the cluster using the supplied executor. If {@code queryNumbers} is null,
     * auto-discovers all queries. Collects failures and returns them (does not fail-fast).
     */
    public static List<String> runQueries(
        RestClient client,
        Dataset dataset,
        String language,
        String extension,
        List<Integer> queryNumbers,
        QueryExecutor executor
    ) {
        List<Integer> queriesToRun = queryNumbers;
        if (queriesToRun == null) {
            try {
                queriesToRun = discoverQueryNumbers(dataset, language);
            } catch (IOException e) {
                return Collections.singletonList("Failed to discover queries: " + e.getMessage());
            }
            if (queriesToRun.isEmpty()) {
                logger.warn("No queries discovered for dataset [{}] language [{}]", dataset.name, language);
                return Collections.emptyList();
            }
        }

        List<String> failures = new ArrayList<>();
        for (int queryNum : queriesToRun) {
            String queryId = language.toUpperCase(Locale.ROOT) + " Q" + queryNum;
            try {
                String queryBody = DatasetProvisioner.loadResource(dataset.queryResourcePath(language, extension, queryNum));
                Map<String, Object> response = executor.execute(client, dataset, queryBody);
                if (response == null || response.isEmpty()) {
                    failures.add(queryId + ": empty response");
                }
            } catch (Exception e) {
                failures.add(queryId + " failed: " + e.getMessage());
            }
        }
        return failures;
    }
}
