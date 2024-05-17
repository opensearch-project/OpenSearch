/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata for legacy templates
 *
 * @opensearch.api
 */
@PublicApi(since = "2.15.0")
public class TemplatesMetadata extends AbstractDiffable<TemplatesMetadata> implements ToXContentFragment {
    public static TemplatesMetadata EMPTY_METADATA = builder().build();
    private final Map<String, IndexTemplateMetadata> templates;

    public TemplatesMetadata() {
        this(Collections.emptyMap());
    }

    public TemplatesMetadata(Map<String, IndexTemplateMetadata> templates) {
        this.templates = Collections.unmodifiableMap(templates);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, IndexTemplateMetadata> getTemplates() {
        return this.templates;
    }

    public static TemplatesMetadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(templates.size());
        for (final IndexTemplateMetadata cursor : templates.values()) {
            cursor.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemplatesMetadata that = (TemplatesMetadata) o;

        return Objects.equals(templates, that.templates);
    }

    @Override
    public int hashCode() {
        return templates != null ? templates.hashCode() : 0;
    }

    /**
     * Builder for the templates metadata
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.15.0")
    public static class Builder {
        private final Map<String, IndexTemplateMetadata> templates;

        public Builder() {
            this.templates = new HashMap<String, IndexTemplateMetadata>();
        }

        public Builder(Map<String, IndexTemplateMetadata> templates) {
            this.templates = templates;
        }

        public Builder put(IndexTemplateMetadata.Builder templateBuilder) {
            return put(templateBuilder.build());
        }

        public Builder put(IndexTemplateMetadata template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            this.templates.putAll(templates);
            return this;
        }

        public TemplatesMetadata build() {
            return new TemplatesMetadata(templates);
        }

        public static void toXContent(TemplatesMetadata templatesMetadata, XContentBuilder builder, Params params) throws IOException {
            for (IndexTemplateMetadata cursor : templatesMetadata.getTemplates().values()) {
                IndexTemplateMetadata.Builder.toXContentWithTypes(cursor, builder, params);
            }
        }

        public static TemplatesMetadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (currentFieldName == null) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    // move to the field name
                    token = parser.nextToken();
                }
                currentFieldName = parser.currentName();
            }
            if (currentFieldName != null) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                }
            }
            return builder.build();
        }
    }
}
