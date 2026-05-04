/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

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
import java.util.Locale;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Generic runner that discovers queries from a dataset's resource directory and
 * executes them against a live cluster.
 * <p>
 * For a dataset at {@code resources/datasets/{name}/}, queries are auto-discovered
 * from {@code {language}/} and executed via the provided {@link QueryExecutor}.
 */
public final class DatasetQueryRunner {

    private static final Logger logger = LogManager.getLogger(DatasetQueryRunner.class);
    private static final Pattern QUERY_FILE_PATTERN = Pattern.compile("q(\\d+)\\.\\w+");

    /** Executes a single query against a live cluster and returns the response body as a Map. */
    @FunctionalInterface
    public interface QueryExecutor {
        Map<String, Object> execute(RestClient client, Dataset dataset, String queryBody) throws IOException;
    }

    private DatasetQueryRunner() {
        // utility class
    }

    /**
     * Discover all query numbers available for the given dataset and language.
     * Returns a sorted list of query numbers N such that {@code {language}/q{N}.{ext}} exists.
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
                    String fileName = p.getFileName().toString();
                    Matcher m = QUERY_FILE_PATTERN.matcher(fileName);
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
     * Run the given query numbers against the cluster using the supplied executor.
     * Collects failures and returns them as a list — does not fail-fast so all queries are attempted.
     *
     * @param client       the REST client
     * @param dataset      the dataset descriptor
     * @param language     the query language directory (e.g. "dsl", "ppl")
     * @param extension    the query file extension (e.g. "json", "ppl")
     * @param queryNumbers the query numbers to run
     * @param executor     the executor that sends the query to the cluster
     * @return list of failure messages (empty if all queries succeeded)
     */
    public static List<String> runQueries(
        RestClient client,
        Dataset dataset,
        String language,
        String extension,
        List<Integer> queryNumbers,
        QueryExecutor executor
    ) {
        List<String> failures = new ArrayList<>();
        for (int queryNum : queryNumbers) {
            String queryId = language.toUpperCase(Locale.ROOT) + " Q" + queryNum;
            try {
                String queryBody = DatasetProvisioner.loadResource(dataset.queryResourcePath(language, extension, queryNum));
                logger.info("=== {} ===\n{}", queryId, queryBody);

                Map<String, Object> response = executor.execute(client, dataset, queryBody);
                logger.info("{} response: {}", queryId, response);

                if (response == null || response.isEmpty()) {
                    failures.add(queryId + ": empty response");
                }
            } catch (Exception e) {
                String msg = queryId + " failed: " + e.getMessage();
                logger.error(msg, e);
                failures.add(msg);
            }
        }
        return failures;
    }
}
