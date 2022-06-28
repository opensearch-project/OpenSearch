/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.knn;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class DenseVectorIT extends OpenSearchIntegTestCase {

    private static final float[] VECTOR_ONE = { 2.0f, 4.5f, 5.6f, 4.2f };
    private static final float[] VECTOR_TWO = { 4.0f, 2.5f, 1.6f, 2.2f };

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIndexingSingleDocumentWithoutKnn() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder defaultMapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("vector_field")
            .field("type", "dense_vector")
            .field("dimension", 4)
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(defaultMapping));
        ensureGreen();

        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("vector_field", VECTOR_ONE).endObject())
        );
        ensureSearchable("test");
    }

    public void testIndexingSingleDocumentWithDefaultKnnParams() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder defaultMapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("vector_field")
            .field("type", "dense_vector")
            .field("dimension", 4)
            .field("knn", Map.of())
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(defaultMapping));
        ensureGreen();

        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("vector_field", VECTOR_ONE).endObject())
        );
        ensureSearchable("test");
    }

    public void testIndexingMultipleDocumentsWithHnswDefinition() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder defaultMapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "dense_vector")
            .field("dimension", 4)
            .field(
                "knn",
                Map.of("metric", "l2", "algorithm", Map.of("name", "hnsw", "parameters", Map.of("max_connections", 12, "beam_width", 256)))
            )
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(defaultMapping));
        ensureGreen();

        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("vector_field", VECTOR_ONE).endObject()),
            client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("vector_field", VECTOR_TWO).endObject())
        );
        ensureSearchable("test");
    }
}
