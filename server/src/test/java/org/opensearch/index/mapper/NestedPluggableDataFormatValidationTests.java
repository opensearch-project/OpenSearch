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

/**
 * Unit tests for the mapping-time validations applied to fields declared directly inside a
 * {@code nested} object on a composite (pluggable data format / Mustang) index:
 * <ol>
 *   <li>an explicit {@code index: true} is rejected — on the field itself, or on any of its
 *       multi-fields. (A field's default/unspecified {@code index} value, e.g. keyword's default of
 *       {@code index: true}, is deliberately NOT checked — nested leaves are doc-values-only in this
 *       mode regardless of that setting, by design, so the common shape of a leaf with no explicit
 *       "index" key at all must keep working.);</li>
 *   <li>a plain {@code object} sub-field is rejected, whether written explicitly (a {@code properties}
 *       block), implicitly (a bare {@code properties} block with no {@code type}), or via a dotted
 *       field name that implicitly builds the same intermediate object wrapper.</li>
 * </ol>
 * Both are gated on {@link org.opensearch.index.mapper.Mapper#isPluggableDataFormatEnabled} so that
 * vanilla (non-pluggable) indices keep their existing behavior — the vanilla cases below assert the
 * identical mappings still parse successfully.
 *
 * @see ObjectMapper.TypeParser#parseProperties
 */
public class NestedPluggableDataFormatValidationTests extends MapperServiceTestCase {

    private static final Settings PLUGGABLE = Settings.builder().put("index.pluggable.dataformat.enabled", true).build();

    /** Builds a {@code _doc} mapping with a single {@code nested} field {@code n} whose sole sub-field is supplied by the caller. */
    private XContentBuilder nestedWith(CheckedConsumer<XContentBuilder, IOException> subField) throws IOException {
        return mapping(b -> {
            b.startObject("n");
            b.field("type", "nested");
            b.startObject("properties");
            subField.accept(b);
            b.endObject();
            b.endObject();
        });
    }

    // ---- Mustang (pluggable data format): rejections -------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexTrueInsideNestedRejectedForPluggable() throws IOException {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                PLUGGABLE,
                nestedWith(b -> b.startObject("a").field("type", "keyword").field("index", true).endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Field [a] inside nested field [n]"));
        assertThat(e.getMessage(), containsString("index: true"));
        assertThat(e.getMessage(), containsString("not supported on composite (pluggable data format)"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testObjectInsideNestedRejectedForPluggable() throws IOException {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(PLUGGABLE, nestedWith(b -> {
                b.startObject("obj").field("type", "object").startObject("properties");
                b.startObject("x").field("type", "keyword").endObject();
                b.endObject().endObject();
            }))
        );
        assertThat(e.getMessage(), containsString("Object field [obj] inside nested field [n]"));
        assertThat(e.getMessage(), containsString("not supported on composite (pluggable data format)"));
    }

    /** An implicit object (a bare {@code properties} block with no {@code type}) resolves to CONTENT_TYPE and is rejected too. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testImplicitObjectInsideNestedRejectedForPluggable() throws IOException {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(PLUGGABLE, nestedWith(b -> {
                b.startObject("obj").startObject("properties");
                b.startObject("x").field("type", "keyword").endObject();
                b.endObject().endObject();
            }))
        );
        assertThat(e.getMessage(), containsString("Object field [obj] inside nested field [n]"));
    }

    // ---- Mustang (pluggable data format): allowed shapes ---------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testFlatObjectInsideNestedAllowedForPluggable() throws IOException {
        // flat_object is the intended container for open key spaces inside nested — must be accepted.
        createDocumentMapper(PLUGGABLE, nestedWith(b -> b.startObject("meta").field("type", "flat_object").endObject()));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexFalseInsideNestedAllowedForPluggable() throws IOException {
        // Only an explicit index:true is rejected; index:false is the supported (doc-values-only) shape.
        createDocumentMapper(PLUGGABLE, nestedWith(b -> b.startObject("a").field("type", "keyword").field("index", false).endObject()));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDefaultLeafInsideNestedAllowedForPluggable() throws IOException {
        // A leaf with no explicit index parameter must still be accepted (rule keys off explicit
        // index:true only — see the class-level javadoc for why the default is deliberately not checked).
        createDocumentMapper(PLUGGABLE, nestedWith(b -> b.startObject("a").field("type", "keyword").endObject()));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDottedFieldNameInsideNestedRejectedForPluggable() throws IOException {
        // "meta.name" implicitly builds an intermediate plain-object mapper for "meta" — the same
        // disallowed shape as the explicit-object case, just via dotted-name sugar.
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(PLUGGABLE, nestedWith(b -> b.startObject("meta.name").field("type", "keyword").endObject()))
        );
        assertThat(e.getMessage(), containsString("Field [meta.name] inside nested field [n]"));
        assertThat(e.getMessage(), containsString("dotted name"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMultiFieldIndexTrueInsideNestedRejectedForPluggable() throws IOException {
        // The parent ("author") sets index:false — only its multi-field ("raw") sets explicit index:true
        // — so the check must inspect multi-fields' own "index" property too, not just the parent's.
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(PLUGGABLE, nestedWith(b -> {
                b.startObject("author").field("type", "keyword").field("index", false).startObject("fields");
                b.startObject("raw").field("type", "keyword").field("index", true).endObject();
                b.endObject().endObject();
            }))
        );
        assertThat(e.getMessage(), containsString("Field [author.raw] inside nested field [n]"));
        assertThat(e.getMessage(), containsString("index: true"));
    }

    // ---- Vanilla: behavior unchanged (identical mappings must still parse) ----------------------

    public void testIndexTrueInsideNestedAllowedForVanilla() throws IOException {
        // No pluggable setting and no feature flag → validation must not fire.
        createDocumentMapper(nestedWith(b -> b.startObject("a").field("type", "keyword").field("index", true).endObject()));
    }

    public void testObjectInsideNestedAllowedForVanilla() throws IOException {
        createDocumentMapper(nestedWith(b -> {
            b.startObject("obj").field("type", "object").startObject("properties");
            b.startObject("x").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
    }

    public void testDottedFieldNameInsideNestedAllowedForVanilla() throws IOException {
        createDocumentMapper(nestedWith(b -> b.startObject("meta.name").field("type", "keyword").endObject()));
    }

    public void testMultiFieldIndexTrueInsideNestedAllowedForVanilla() throws IOException {
        createDocumentMapper(nestedWith(b -> {
            b.startObject("author").field("type", "keyword").field("index", false).startObject("fields");
            b.startObject("raw").field("type", "keyword").field("index", true).endObject();
            b.endObject().endObject();
        }));
    }
}
