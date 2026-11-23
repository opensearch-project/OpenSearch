/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.Base64;

import static org.hamcrest.Matchers.equalTo;

public class ScriptFieldsSourceIT extends OpenSearchRestTestCase {

    public void testScriptFieldReadsSourceFromCompoundSegment() throws IOException {
        Request createIndex = new Request("PUT", "/compound-source");
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index.use_compound_file": true,
                "index.store.type": "mmapfs",
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0
              },
              "mappings": { "dynamic": false }
            }
            """);
        assertOK(client().performRequest(createIndex));

        // Keep the compressed stored fields large enough for mmap read advice to apply.
        byte[] padding = randomByteArrayOfLength(256 * 1024);
        Request index = new Request("PUT", "/compound-source/_doc/1");
        index.addParameter("refresh", "true");
        index.setJsonEntity("{\"product_id\":42,\"padding\":\"" + Base64.getEncoder().encodeToString(padding) + "\"}");
        assertOK(client().performRequest(index));

        Request search = new Request("POST", "/compound-source/_search");
        search.setJsonEntity("""
            {
              "_source": false,
              "script_fields": {
                "product_id": { "script": { "lang": "painless", "source": "return params._source.product_id;" } }
              }
            }
            """);
        Response response = client().performRequest(search);
        assertThat(ObjectPath.createFromResponse(response).evaluate("hits.hits.0.fields.product_id.0"), equalTo(42));
    }
}
