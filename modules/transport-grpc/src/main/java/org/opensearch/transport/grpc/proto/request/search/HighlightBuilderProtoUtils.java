/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoundaryScanner;
import org.opensearch.protobufs.BuiltinHighlighterType;
import org.opensearch.protobufs.Highlight;
import org.opensearch.protobufs.HighlighterEncoder;
import org.opensearch.protobufs.HighlighterFragmenter;
import org.opensearch.protobufs.HighlighterOrder;
import org.opensearch.protobufs.HighlighterTagsSchema;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for converting Highlight Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of highlights
 * into their corresponding OpenSearch HighlightBuilder implementations for search result highlighting.
 */
class HighlightBuilderProtoUtils {

    private HighlightBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer Highlight to an OpenSearch HighlightBuilder.
     * Similar to {@link HighlightBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * HighlightBuilder with the appropriate settings.
     *
     * @param highlightProto The Protocol Buffer Highlight to convert
     * @param registry The registry for converting highlight queries
     * @return A configured HighlightBuilder instance
     * @throws IllegalArgumentException if highlightProto is null
     */
    static HighlightBuilder fromProto(Highlight highlightProto, QueryBuilderProtoConverterRegistry registry) {
        if (highlightProto == null) {
            throw new IllegalArgumentException("Highlight cannot be null");
        }

        HighlightBuilder highlightBuilder = new HighlightBuilder();

        if (highlightProto.getPreTagsCount() > 0) {
            String[] preTags = new String[highlightProto.getPreTagsCount()];
            for (int i = 0; i < highlightProto.getPreTagsCount(); i++) {
                preTags[i] = highlightProto.getPreTags(i);
            }
            highlightBuilder.preTags(preTags);
        }

        if (highlightProto.getPostTagsCount() > 0) {
            String[] postTags = new String[highlightProto.getPostTagsCount()];
            for (int i = 0; i < highlightProto.getPostTagsCount(); i++) {
                postTags[i] = highlightProto.getPostTags(i);
            }
            highlightBuilder.postTags(postTags);
        }

        if (highlightProto.hasOrder() && highlightProto.getOrder() == HighlighterOrder.HIGHLIGHTER_ORDER_SCORE) {
            highlightBuilder.order(HighlightBuilder.Order.SCORE);
        }

        if (highlightProto.hasHighlightFilter()) {
            highlightBuilder.highlightFilter(highlightProto.getHighlightFilter());
        }

        if (highlightProto.hasFragmentSize()) {
            highlightBuilder.fragmentSize(highlightProto.getFragmentSize());
        }

        if (highlightProto.hasNumberOfFragments()) {
            highlightBuilder.numOfFragments(highlightProto.getNumberOfFragments());
        }

        if (highlightProto.hasRequireFieldMatch()) {
            highlightBuilder.requireFieldMatch(highlightProto.getRequireFieldMatch());
        }

        if (highlightProto.hasBoundaryScanner() && highlightProto.getBoundaryScanner() != BoundaryScanner.BOUNDARY_SCANNER_UNSPECIFIED) {
            HighlightBuilder.BoundaryScannerType boundaryScanner = parseBoundaryScanner(highlightProto.getBoundaryScanner());
            highlightBuilder.boundaryScannerType(boundaryScanner);
        }

        if (highlightProto.hasBoundaryMaxScan()) {
            highlightBuilder.boundaryMaxScan(highlightProto.getBoundaryMaxScan());
        }

        if (highlightProto.hasBoundaryChars()) {
            highlightBuilder.boundaryChars(highlightProto.getBoundaryChars().toCharArray());
        }

        if (highlightProto.hasBoundaryScannerLocale()) {
            highlightBuilder.boundaryScannerLocale(highlightProto.getBoundaryScannerLocale());
        }

        if (highlightProto.hasType()) {
            if (highlightProto.getType().hasBuiltin()
                && highlightProto.getType().getBuiltin() != BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_UNSPECIFIED) {
                highlightBuilder.highlighterType(ProtobufEnumUtils.convertToString(highlightProto.getType().getBuiltin()));
            } else if (highlightProto.getType().hasCustom()) {
                highlightBuilder.highlighterType(highlightProto.getType().getCustom());
            }
        }

        if (highlightProto.hasFragmenter() && highlightProto.getFragmenter() != HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_UNSPECIFIED) {
            highlightBuilder.fragmenter(ProtobufEnumUtils.convertToString(highlightProto.getFragmenter()));
        }

        if (highlightProto.hasNoMatchSize()) {
            highlightBuilder.noMatchSize(highlightProto.getNoMatchSize());
        }

        if (highlightProto.hasForceSource()) {
            highlightBuilder.forceSource(highlightProto.getForceSource());
        }

        if (highlightProto.hasPhraseLimit()) {
            highlightBuilder.phraseLimit(highlightProto.getPhraseLimit());
        }

        if (highlightProto.hasMaxAnalyzerOffset()) {
            highlightBuilder.maxAnalyzerOffset(highlightProto.getMaxAnalyzerOffset());
        }

        if (highlightProto.hasOptions()) {
            highlightBuilder.options(ObjectMapProtoUtils.fromProto(highlightProto.getOptions()));
        }

        if (highlightProto.hasHighlightQuery()) {
            if (registry == null) {
                throw new IllegalStateException("QueryBuilderProtoConverterRegistry cannot be null.");
            }
            QueryBuilder query = registry.fromProto(highlightProto.getHighlightQuery());
            highlightBuilder.highlightQuery(query);
        }

        if (highlightProto.hasTagsSchema() && highlightProto.getTagsSchema() != HighlighterTagsSchema.HIGHLIGHTER_TAGS_SCHEMA_UNSPECIFIED) {
            highlightBuilder.tagsSchema(ProtobufEnumUtils.convertToString(highlightProto.getTagsSchema()));
        }

        if (highlightProto.hasEncoder() && highlightProto.getEncoder() != HighlighterEncoder.HIGHLIGHTER_ENCODER_UNSPECIFIED) {
            highlightBuilder.encoder(ProtobufEnumUtils.convertToString(highlightProto.getEncoder()));
        }

        // TODO: Support useExplicitFieldOrder
        // A spec fix is required and a corresponding protobuf version upgrade.

        if (highlightProto.getFieldsCount() > 0) {
            for (java.util.Map.Entry<String, org.opensearch.protobufs.HighlightField> entry : highlightProto.getFieldsMap().entrySet()) {
                String fieldName = entry.getKey();
                org.opensearch.protobufs.HighlightField fieldProto = entry.getValue();

                HighlightBuilder.Field fieldBuilder = new HighlightBuilder.Field(fieldName);

                if (fieldProto.hasFragmentOffset()) {
                    fieldBuilder.fragmentOffset(fieldProto.getFragmentOffset());
                }

                if (fieldProto.getMatchedFieldsCount() > 0) {
                    String[] matchedFields = new String[fieldProto.getMatchedFieldsCount()];
                    for (int j = 0; j < fieldProto.getMatchedFieldsCount(); j++) {
                        matchedFields[j] = fieldProto.getMatchedFields(j);
                    }
                    fieldBuilder.matchedFields(matchedFields);
                }

                if (fieldProto.getPreTagsCount() > 0) {
                    String[] preTags = new String[fieldProto.getPreTagsCount()];
                    for (int j = 0; j < fieldProto.getPreTagsCount(); j++) {
                        preTags[j] = fieldProto.getPreTags(j);
                    }
                    fieldBuilder.preTags(preTags);
                }

                if (fieldProto.getPostTagsCount() > 0) {
                    String[] postTags = new String[fieldProto.getPostTagsCount()];
                    for (int j = 0; j < fieldProto.getPostTagsCount(); j++) {
                        postTags[j] = fieldProto.getPostTags(j);
                    }
                    fieldBuilder.postTags(postTags);
                }

                if (fieldProto.hasType()) {
                    if (fieldProto.getType().hasBuiltin()
                        && fieldProto.getType().getBuiltin() != BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_UNSPECIFIED) {
                        fieldBuilder.highlighterType(ProtobufEnumUtils.convertToString(fieldProto.getType().getBuiltin()));
                    } else if (fieldProto.getType().hasCustom()) {
                        fieldBuilder.highlighterType(fieldProto.getType().getCustom());
                    }
                }

                if (fieldProto.hasBoundaryChars()) {
                    fieldBuilder.boundaryChars(fieldProto.getBoundaryChars().toCharArray());
                }

                if (fieldProto.hasBoundaryMaxScan()) {
                    fieldBuilder.boundaryMaxScan(fieldProto.getBoundaryMaxScan());
                }

                if (fieldProto.hasBoundaryScanner() && fieldProto.getBoundaryScanner() != BoundaryScanner.BOUNDARY_SCANNER_UNSPECIFIED) {
                    HighlightBuilder.BoundaryScannerType boundaryScanner = parseBoundaryScanner(fieldProto.getBoundaryScanner());
                    fieldBuilder.boundaryScannerType(boundaryScanner);
                }

                if (fieldProto.hasBoundaryScannerLocale()) {
                    fieldBuilder.boundaryScannerLocale(fieldProto.getBoundaryScannerLocale());
                }

                if (fieldProto.hasFragmenter() && fieldProto.getFragmenter() != HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_UNSPECIFIED) {
                    fieldBuilder.fragmenter(ProtobufEnumUtils.convertToString(fieldProto.getFragmenter()));
                }

                if (fieldProto.hasFragmentSize()) {
                    fieldBuilder.fragmentSize(fieldProto.getFragmentSize());
                }

                if (fieldProto.hasHighlightFilter()) {
                    fieldBuilder.highlightFilter(fieldProto.getHighlightFilter());
                }

                if (fieldProto.hasHighlightQuery()) {
                    if (registry == null) {
                        throw new IllegalStateException("QueryBuilderProtoConverterRegistry cannot be null.");
                    }
                    QueryBuilder query = registry.fromProto(fieldProto.getHighlightQuery());
                    fieldBuilder.highlightQuery(query);
                }

                if (fieldProto.hasNoMatchSize()) {
                    fieldBuilder.noMatchSize(fieldProto.getNoMatchSize());
                }

                if (fieldProto.hasNumberOfFragments()) {
                    fieldBuilder.numOfFragments(fieldProto.getNumberOfFragments());
                }

                if (fieldProto.hasOptions()) {
                    fieldBuilder.options(ObjectMapProtoUtils.fromProto(fieldProto.getOptions()));
                }
                if (fieldProto.hasMaxAnalyzerOffset()) {
                    fieldBuilder.maxAnalyzerOffset(fieldProto.getMaxAnalyzerOffset());
                }

                if (fieldProto.hasForceSource()) {
                    fieldBuilder.forceSource(fieldProto.getForceSource());
                }

                if (fieldProto.hasOrder() && fieldProto.getOrder() == HighlighterOrder.HIGHLIGHTER_ORDER_SCORE) {
                    fieldBuilder.order(HighlightBuilder.Order.SCORE);
                }

                if (fieldProto.hasPhraseLimit()) {
                    fieldBuilder.phraseLimit(fieldProto.getPhraseLimit());
                }

                if (fieldProto.hasRequireFieldMatch()) {
                    fieldBuilder.requireFieldMatch(fieldProto.getRequireFieldMatch());
                }

                if (fieldProto.hasTagsSchema() && fieldProto.getTagsSchema() != HighlighterTagsSchema.HIGHLIGHTER_TAGS_SCHEMA_UNSPECIFIED) {
                    applyTagsSchemaToField(fieldBuilder, fieldProto.getTagsSchema());
                }

                highlightBuilder.field(fieldBuilder);
            }
        }

        return highlightBuilder;
    }

    /**
     * Convert protobuf BoundaryScanner enum to OpenSearch BoundaryScannerType enum
     */
    private static HighlightBuilder.BoundaryScannerType parseBoundaryScanner(BoundaryScanner boundaryScanner) {
        switch (boundaryScanner) {
            case BOUNDARY_SCANNER_CHARS:
                return HighlightBuilder.BoundaryScannerType.CHARS;
            case BOUNDARY_SCANNER_WORD:
                return HighlightBuilder.BoundaryScannerType.WORD;
            case BOUNDARY_SCANNER_SENTENCE:
                return HighlightBuilder.BoundaryScannerType.SENTENCE;
            default:
                throw new IllegalArgumentException("Unknown BoundaryScanner value: " + boundaryScanner);
        }
    }

    /**
     * Apply tags schema to a highlight field by setting pre/post tags.
     * This mirrors the behavior of {@link HighlightBuilder#tagsSchema(String)} but for field-level settings.
     * <p>
     * The {@code tagsSchema} method on {@link HighlightBuilder} sets built-in pre and post tags based on a schema name.
     * Since {@link HighlightBuilder.Field} doesn't have a {@code tagsSchema} method, this helper applies the same
     * logic by directly setting the pre/post tags on the field.
     *
     * @param fieldBuilder the highlight field builder to apply the tags schema to
     * @param tagsSchema the tags schema to apply (e.g., STYLED)
     * @see HighlightBuilder#tagsSchema(String)
     */
    private static void applyTagsSchemaToField(HighlightBuilder.Field fieldBuilder, HighlighterTagsSchema tagsSchema) {
        switch (tagsSchema) {
            case HIGHLIGHTER_TAGS_SCHEMA_STYLED:
                fieldBuilder.preTags(HighlightBuilder.DEFAULT_STYLED_PRE_TAG);
                fieldBuilder.postTags(HighlightBuilder.DEFAULT_STYLED_POST_TAGS);
                break;
            case HIGHLIGHTER_TAGS_SCHEMA_DEFAULT:
                fieldBuilder.preTags(HighlightBuilder.DEFAULT_PRE_TAGS);
                fieldBuilder.postTags(HighlightBuilder.DEFAULT_POST_TAGS);
                break;
            default:
                throw new IllegalArgumentException("Unknown tags schema: " + tagsSchema);
        }
    }
}
