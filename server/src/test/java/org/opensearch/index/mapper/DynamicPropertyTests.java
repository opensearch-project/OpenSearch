/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DynamicPropertyTests extends OpenSearchTestCase {

    public void testMatchesFullDottedPath() {
        DynamicProperty dp = new DynamicProperty("*_i", Map.of("type", "long"));
        assertTrue(dp.matches("count_i"));
        assertTrue(dp.matches("parent.child_i"));
        assertFalse(dp.matches("count_j"));
    }

    public void testMappingForNameSubstitutesLeafInKeysAndStringValues() {
        Map<String, Object> nested = new TreeMap<>();
        nested.put("path", "meta_{name}_suffix");
        Map<String, Object> mapping = new TreeMap<>();
        mapping.put("type", "keyword");
        mapping.put("{name}_subfield", Map.of("type", "keyword"));
        mapping.put("meta", nested);
        DynamicProperty dp = new DynamicProperty("*_s", mapping);
        Map<String, Object> resolved = dp.mappingForName("name_s");
        assertThat(resolved.get("type"), equalTo("keyword"));
        assertTrue(resolved.containsKey("name_s_subfield"));
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) resolved.get("meta");
        assertThat(meta.get("path"), equalTo("meta_name_s_suffix"));
    }

    public void testMappingForNameRejectsNonStringNestedKeys() {
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "keyword");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Map badNested = new HashMap();
        badNested.put(1, "x");
        mapping.put("fields", badNested);
        DynamicProperty dp = new DynamicProperty("*_k", mapping);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> dp.mappingForName("a_k"));
        assertThat(e.getMessage(), containsString("string keys"));
    }

    public void testToXContent() throws IOException {
        DynamicProperty dp = new DynamicProperty("*_t", Map.of("type", "text"));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        dp.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertThat(builder.toString(), containsString("\"*_t\""));
        assertThat(builder.toString(), containsString("\"type\":\"text\""));
    }

    public void testGetMapping() {
        Map<String, Object> m = new TreeMap<>(Map.of("type", "keyword", "doc_values", Boolean.FALSE));
        DynamicProperty dp = new DynamicProperty("*_k", m);
        assertThat(dp.getMapping().get("type"), equalTo("keyword"));
        assertThat(dp.getMapping().get("doc_values"), equalTo(Boolean.FALSE));
    }

    /** Lists and other non-map, non-string values are stored unchanged by {@code mappingForName}. */
    public void testMappingForNameLeavesListValuesUnsubstituted() {
        Map<String, Object> mapping = new TreeMap<>();
        mapping.put("type", "keyword");
        mapping.put("copy_to", List.of("dest_a", "dest_b"));
        DynamicProperty dp = new DynamicProperty("*_s", mapping);
        Map<String, Object> resolved = dp.mappingForName("name_s");
        assertThat(resolved.get("type"), equalTo("keyword"));
        @SuppressWarnings("unchecked")
        List<String> copyTo = (List<String>) resolved.get("copy_to");
        assertThat(copyTo, equalTo(List.of("dest_a", "dest_b")));
    }
}
