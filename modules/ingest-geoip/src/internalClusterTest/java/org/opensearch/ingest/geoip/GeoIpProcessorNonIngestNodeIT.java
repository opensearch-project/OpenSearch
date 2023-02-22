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

package org.opensearch.ingest.geoip;

import org.apache.lucene.util.Constants;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.ingest.IngestService;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.NodeRoles;
import org.opensearch.test.StreamsUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.test.NodeRoles.nonIngestNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class GeoIpProcessorNonIngestNodeIT extends OpenSearchIntegTestCase {

    public static class IngestGeoIpSettingsPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(Setting.simpleString("ingest.geoip.database_path", Setting.Property.NodeScope));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestGeoIpModulePlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {
        final Path databasePath = createTempDir();
        try {
            Files.createDirectories(databasePath);
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")),
                databasePath.resolve("GeoLite2-City.mmdb")
            );
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")),
                databasePath.resolve("GeoLite2-Country.mmdb")
            );
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-ASN.mmdb")),
                databasePath.resolve("GeoLite2-ASN.mmdb")
            );
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return Settings.builder()
            .put("ingest.geoip.database_path", databasePath)
            .put(nonIngestNode())
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    /**
     * This test shows that we do not load the geo-IP databases on non-ingest nodes, and only load on ingest nodes on first use.
     *
     * @throws IOException if an I/O exception occurs building the JSON
     */
    public void testLazyLoading() throws IOException {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/37342", Constants.WINDOWS);
        final BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.field("description", "test");
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-country");
                            builder.field("database_file", "GeoLite2-Country.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-asn");
                            builder.field("database_file", "GeoLite2-ASN.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(client().admin().cluster().putPipeline(new PutPipelineRequest("geoip", bytes, XContentType.JSON)).actionGet());
        // the geo-IP databases should not be loaded on any nodes as they are all non-ingest nodes
        Arrays.stream(internalCluster().getNodeNames()).forEach(node -> assertDatabaseLoadStatus(node, false));

        // start an ingest node
        final String ingestNode = internalCluster().startNode(NodeRoles.ingestNode());
        internalCluster().getInstance(IngestService.class, ingestNode);
        // the geo-IP database should not be loaded yet as we have no indexed any documents using a pipeline that has a geo-IP processor
        assertDatabaseLoadStatus(ingestNode, false);
        final IndexRequest indexRequest = new IndexRequest("index");
        indexRequest.setPipeline("geoip");
        indexRequest.source(Collections.singletonMap("ip", "1.1.1.1"));
        final IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
        // now the geo-IP database should be loaded on the ingest node
        assertDatabaseLoadStatus(ingestNode, true);
        // the geo-IP database should still not be loaded on the non-ingest nodes
        Arrays.stream(internalCluster().getNodeNames())
            .filter(node -> node.equals(ingestNode) == false)
            .forEach(node -> assertDatabaseLoadStatus(node, false));
    }

    private void assertDatabaseLoadStatus(final String node, final boolean loaded) {
        final IngestService ingestService = internalCluster().getInstance(IngestService.class, node);
        final GeoIpProcessor.Factory factory = (GeoIpProcessor.Factory) ingestService.getProcessorFactories().get("geoip");
        for (final DatabaseReaderLazyLoader loader : factory.databaseReaders().values()) {
            if (loaded) {
                assertNotNull(loader.databaseReader.get());
            } else {
                assertNull(loader.databaseReader.get());
            }
        }
    }

}
