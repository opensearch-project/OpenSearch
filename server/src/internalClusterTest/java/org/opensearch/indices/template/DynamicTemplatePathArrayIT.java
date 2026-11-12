/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.template;

import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class DynamicTemplatePathArrayIT extends OpenSearchIntegTestCase {

    public void testLegacyTemplate() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("paths")
                .setPatterns(List.of("paths-*"))
                .setMapping(mapping(), MediaTypeRegistry.JSON)
                .get()
        );
        assertMapping("paths-legacy");
    }

    public void testComponentTemplate() throws Exception {
        ComponentTemplate t = new ComponentTemplate(new Template(null, new CompressedXContent(mapping()), null), null, null);
        assertAcked(
            client().execute(
                PutComponentTemplateAction.INSTANCE,
                new PutComponentTemplateAction.Request("paths-component").componentTemplate(t)
            ).get()
        );
        ComposableIndexTemplate i = new ComposableIndexTemplate(List.of("paths-*"), null, List.of("paths-component"), null, null, null);
        assertAcked(
            client().execute(
                PutComposableIndexTemplateAction.INSTANCE,
                new PutComposableIndexTemplateAction.Request("paths").indexTemplate(i)
            ).get()
        );
        assertMapping("paths-component");
    }

    private String mapping() {
        return """
            {
              "dynamic_templates": [
                {
                  "paths": {
                    "match_mapping_type": "string",
                    "path_match": ["a.*", "b.*"],
                    "path_unmatch": ["a.skip", "b.skip"],
                    "mapping": {"type": "keyword"}
                  }
                }
              ]
            }
            """;
    }

    private void assertMapping(String i) throws Exception {
        client().prepareIndex(i)
            .setId("1")
            .setSource(Map.of("a", Map.of("value", "a", "skip", "s"), "b", Map.of("value", "b", "skip", "s"), "c", Map.of("value", "c")))
            .get();
        Map<String, Object> m = client().admin().indices().prepareGetMappings(i).get().getMappings().get(i).sourceAsMap();
        List<?> t = (List<?>) m.get("dynamic_templates");
        Map<?, ?> d = (Map<?, ?>) ((Map<?, ?>) t.get(0)).get("paths");
        assertEquals(List.of("a.*", "b.*"), d.get("path_match"));
        assertEquals(List.of("a.skip", "b.skip"), d.get("path_unmatch"));
        Map<?, ?> p = (Map<?, ?>) m.get("properties");
        for (String n : List.of("a", "b", "c")) {
            Map<?, ?> f = (Map<?, ?>) ((Map<?, ?>) p.get(n)).get("properties");
            assertEquals(n.equals("c") ? "text" : "keyword", ((Map<?, ?>) f.get("value")).get("type"));
            if (n.equals("c") == false) {
                assertEquals("text", ((Map<?, ?>) f.get("skip")).get("type"));
            }
        }
    }
}
