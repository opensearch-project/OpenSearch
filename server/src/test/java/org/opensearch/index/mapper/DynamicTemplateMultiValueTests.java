/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/** Validation of {@code multi_value} in dynamic templates, on pluggable data format and vanilla indices. */
public class DynamicTemplateMultiValueTests extends MapperServiceTestCase {

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRejectedForFlatObjectOnPluggableIndex() throws IOException {
        for (boolean multiValue : new boolean[] { true, false }) {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    pluggableSettings(),
                    template(m -> m.field("type", "flat_object").field("multi_value", multiValue))
                )
            );
            assertThat(
                e.getMessage(),
                containsString("dynamic template [tmpl] maps to type [flat_object], which does not support [multi_value]")
            );
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRejectedForGeoPointOnPluggableIndex() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(pluggableSettings(), template(m -> m.field("type", "geo_point").field("multi_value", true)))
        );
        assertThat(e.getMessage(), containsString("maps to type [geo_point], which does not support [multi_value]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testAcceptedForKeywordOnPluggableIndex() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            template(m -> m.field("type", "keyword").field("multi_value", true))
        );
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"multi_value\":true"));
    }

    /** An invalid sibling parameter is still only a warning, not reported as a multi_value error. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOtherInvalidParameterStillOnlyWarns() throws IOException {
        createMapperService(pluggableSettings(), template(m -> m.field("type", "keyword").field("multi_value", true).field("foo", "bar")));
        assertWarnings(
            "dynamic template [tmpl] has invalid content [{\"match\":\"attrs\",\"mapping\":{\"foo\":\"bar\",\"multi_value\":true,"
                + "\"type\":\"keyword\"}}], caused by [unknown parameter [foo] on mapper [__dynamic__tmpl] of type [keyword]]"
        );
    }

    /** On a vanilla index multi_value is an unknown key like any other: deprecation warning only. */
    public void testOnlyWarnsOnVanillaIndex() throws IOException {
        createMapperService(template(m -> m.field("type", "flat_object").field("multi_value", true)));
        assertWarnings(
            "dynamic template [tmpl] has invalid content [{\"match\":\"attrs\",\"mapping\":{\"multi_value\":true,"
                + "\"type\":\"flat_object\"}}], caused by [Unused mapping attributes [{multi_value=true}]]"
        );
    }

    /** A {name} template is not checked at creation, so the unsupported multi_value is rejected when a document uses it. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNameTemplateRejectedAtDocumentTimeOnPluggableIndex() throws IOException {
        DocumentMapper mapper = createMapperService(pluggableSettings(), nameTemplate()).documentMapper();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("attrs").field("env", "prod").endObject()), new CapturingDocumentInput())
        );
        assertThat(
            e.getMessage(),
            containsString("dynamic template [tmpl] maps to type [flat_object], which does not support [multi_value]")
        );
    }

    public void testNameTemplateIgnoredAtDocumentTimeOnVanillaIndex() throws IOException {
        DocumentMapper mapper = createMapperService(nameTemplate()).documentMapper();
        mapper.parse(source(b -> b.startObject("attrs").field("env", "prod").endObject()));
    }

    private XContentBuilder nameTemplate() throws IOException {
        return template(m -> m.field("type", "flat_object").field("multi_value", true).field("copy_to", "{name}_all"));
    }

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }

    private XContentBuilder template(CheckedConsumer<XContentBuilder, IOException> mapping) throws IOException {
        return topMapping(b -> {
            b.startArray("dynamic_templates").startObject().startObject("tmpl");
            b.field("match", "attrs");
            b.startObject("mapping");
            mapping.accept(b);
            b.endObject();
            b.endObject().endObject().endArray();
        });
    }
}
