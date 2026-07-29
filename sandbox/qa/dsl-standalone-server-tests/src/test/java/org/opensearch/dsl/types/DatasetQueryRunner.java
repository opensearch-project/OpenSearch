/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Discovers DSL query files from a dataset's resource dir. For a dataset at
 * {@code resources/datasets/{name}/}, query numbers are auto-discovered from {@code {language}/q{N}.{ext}}
 * via {@link #discoverQueryNumbers}; {@link DslQueryTypesIT} turns each into a parameterized test.
 *
 * <p>Local copy tailored to this standalone module (no dependency on analytics-engine-rest).
 */
public final class DatasetQueryRunner {

    private static final Pattern QUERY_FILE_PATTERN = Pattern.compile("q(\\d+)\\.\\w+");

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

}
