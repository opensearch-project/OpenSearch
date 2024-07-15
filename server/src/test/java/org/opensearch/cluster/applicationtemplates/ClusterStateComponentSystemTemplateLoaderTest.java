/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.UUID;

public class ClusterStateComponentSystemTemplateLoaderTest extends OpenSearchSingleNodeTestCase {

    public static final String SAMPLE_TEMPLATE = "{\n"
        + "  \"template\": {\n"
        + "    \"settings\": {\n"
        + "       \"index\": {\n"
        + "           \"codec\": \"best_compression\",\n"
        + "           \"merge.policy\": \"log_byte_size\",\n"
        + "           \"refresh_interval\": \"60s\"\n"
        + "       }\n"
        + "    }\n"
        + "  },\n"
        + "  \"_meta\": {\n"
        + "    \"@abc_template\": true,\n"
        + "    \"_version\": 1\n"
        + "  },\n"
        + "  \"version\": 1\n"
        + "}";

    public static final String SAMPLE_TEMPLATE_V2 = "{\n"
        + "  \"template\": {\n"
        + "    \"settings\": {\n"
        + "       \"index\": {\n"
        + "           \"codec\": \"best_compression\",\n"
        + "           \"merge.policy\": \"log_byte_size\",\n"
        + "           \"refresh_interval\": \"60s\"\n"
        + "       }\n"
        + "    }\n"
        + "  },\n"
        + "  \"_meta\": {\n"
        + "    \"@abc_template\": true,\n"
        + "    \"_version\": 2\n"
        + "  },\n"
        + "  \"version\": 2\n"
        + "}";

    public void testLoadTemplate() throws IOException {
        ClusterStateComponentSystemTemplateLoader loader = new ClusterStateComponentSystemTemplateLoader(
            node().client(),
            () -> node().injector().getInstance(ClusterService.class).state()
        );

        TemplateRepositoryMetadata repositoryMetadata = new TemplateRepositoryMetadata(UUID.randomUUID().toString(), 1L);
        SystemTemplateMetadata metadata = SystemTemplateMetadata.fromComponentTemplateInfo("dummy", 1L);

        // Load for the first time
        assertTrue(
            loader.loadTemplate(
                new SystemTemplate(
                    new BytesArray(SAMPLE_TEMPLATE),
                    metadata,
                    new TemplateRepositoryMetadata(UUID.randomUUID().toString(), 1L)
                )
            )
        );
        assertTrue(
            node().injector()
                .getInstance(ClusterService.class)
                .state()
                .metadata()
                .componentTemplates()
                .containsKey(metadata.fullyQualifiedName())
        );
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            SAMPLE_TEMPLATE
        );
        assertEquals(
            node().injector().getInstance(ClusterService.class).state().metadata().componentTemplates().get(metadata.fullyQualifiedName()),
            ComponentTemplate.parse(parser)
        );

        // Retry and ensure loading does not happen again with same version
        assertFalse(
            loader.loadTemplate(
                new SystemTemplate(
                    new BytesArray(SAMPLE_TEMPLATE),
                    metadata,
                    new TemplateRepositoryMetadata(UUID.randomUUID().toString(), 1L)
                )
            )
        );

        // Retry with new template version
        SystemTemplateMetadata newVersionMetadata = SystemTemplateMetadata.fromComponentTemplateInfo("dummy", 2L);
        assertTrue(loader.loadTemplate(new SystemTemplate(new BytesArray(SAMPLE_TEMPLATE_V2), newVersionMetadata, repositoryMetadata)));
        parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            SAMPLE_TEMPLATE_V2
        );
        assertEquals(
            node().injector()
                .getInstance(ClusterService.class)
                .state()
                .metadata()
                .componentTemplates()
                .get(newVersionMetadata.fullyQualifiedName()),
            ComponentTemplate.parse(parser)
        );
    }

    public void testLoadTemplateVersionMismatch() throws IOException {
        ClusterStateComponentSystemTemplateLoader loader = new ClusterStateComponentSystemTemplateLoader(
            node().client(),
            () -> node().injector().getInstance(ClusterService.class).state()
        );

        TemplateRepositoryMetadata repositoryMetadata = new TemplateRepositoryMetadata(UUID.randomUUID().toString(), 1L);
        SystemTemplateMetadata metadata = SystemTemplateMetadata.fromComponentTemplateInfo("dummy", 2L);

        // Load for the first time
        assertThrows(
            OpenSearchCorruptionException.class,
            () -> loader.loadTemplate(
                new SystemTemplate(
                    new BytesArray(SAMPLE_TEMPLATE),
                    metadata,
                    new TemplateRepositoryMetadata(UUID.randomUUID().toString(), 1L)
                )
            )
        );
    }
}
