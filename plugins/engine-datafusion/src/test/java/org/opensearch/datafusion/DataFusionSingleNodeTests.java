/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class DataFusionSingleNodeTests extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_MAPPING_JSON = "clickbench_index_mapping.json";
    private static final String DATA = "clickbench.json";
    private final String indexName = "hits";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataFusionPlugin.class, ParquetDataFormatPlugin.class);
    }

    public void testClickBenchQueries() throws IOException {
        String mappings = fileToString(
            INDEX_MAPPING_JSON,
            false
        );
        createIndexWithMappingSource(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.refresh_interval", -1)
                .build(),
            mappings
        );
        String req = fileToString(
            DATA,
            false
        );
        System.out.println(req.trim());
        client().prepareIndex("hits").setSource(req, MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareRefresh().get();
        client().admin().indices().prepareFlush().get();
        client().admin().indices().prepareFlush().get();

        // TODO: run in a loop
        String sourceFile = fileToString(
            "q7.json",
            false
        );
        SearchSourceBuilder source = new SearchSourceBuilder();
        XContentParser parser = createParser(JsonXContent.jsonXContent,
            sourceFile);
        source.parseXContent(parser);
        SearchResponse response = client().prepareSearch(indexName).setSource(source).get();
        // TODO: Match expected results...
        System.out.println(response);
    }

    static String getResourceFilePath(String relPath) {
        return DataFusionSingleNodeTests.class.getClassLoader().getResource(relPath).getPath();
    }

    static String fileToString(
        final String filePathFromProjectRoot, final boolean removeNewLines) throws IOException {

        final String absolutePath = getResourceFilePath(filePathFromProjectRoot);

        try (final InputStream stream = new FileInputStream(absolutePath);
             final Reader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
             final BufferedReader br = new BufferedReader(streamReader)) {

            final StringBuilder stringBuilder = new StringBuilder();
            String line = br.readLine();

            while (line != null) {

                stringBuilder.append(line);
                if (!removeNewLines) {
                    stringBuilder.append(String.format(Locale.ROOT, "%n"));
                }
                line = br.readLine();
            }

            return stringBuilder.toString();
        }
    }

}
