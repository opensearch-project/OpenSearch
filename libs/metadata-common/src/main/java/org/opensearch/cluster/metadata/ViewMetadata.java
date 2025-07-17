/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.metadata.ComposableIndexTemplateMetadata.MINIMMAL_SUPPORTED_VERSION;

/** View metadata */
@ExperimentalApi
public class ViewMetadata implements Metadata.Custom {

    public static final String TYPE = "view";
    private static final ParseField VIEW_FIELD = new ParseField("view");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ViewMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        a -> new ViewMetadata((Map<String, View>) a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, View> views = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                views.put(p.currentName(), View.fromXContent(p));
            }
            return views;
        }, VIEW_FIELD);
    }

    private final Map<String, View> views;

    public ViewMetadata(final Map<String, View> views) {
        this.views = views;
    }

    public ViewMetadata(final StreamInput in) throws IOException {
        this.views = in.readMap(StreamInput::readString, View::new);
    }

    public Map<String, View> views() {
        return this.views;
    }

    @Override
    public Diff<Metadata.Custom> diff(final Metadata.Custom before) {
        return new ViewMetadata.ViewMetadataDiff((ViewMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(final StreamInput in) throws IOException {
        return new ViewMetadata.ViewMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MINIMMAL_SUPPORTED_VERSION;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeMap(this.views, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public static ViewMetadata fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(VIEW_FIELD.getPreferredName());
        for (Map.Entry<String, View> entry : views.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.views);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ViewMetadata other = (ViewMetadata) obj;
        return Objects.equals(this.views, other.views);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Builder of view metadata.
     */
    @ExperimentalApi
    public static class Builder {

        private final Map<String, View> views = new HashMap<>();

        public Builder putDataStream(final View view) {
            views.put(view.getName(), view);
            return this;
        }

        public ViewMetadata build() {
            return new ViewMetadata(views);
        }
    }

    /**
     * A diff between view metadata.
     */
    static class ViewMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, View>> dataStreamDiff;

        ViewMetadataDiff(ViewMetadata before, ViewMetadata after) {
            this.dataStreamDiff = DiffableUtils.diff(before.views, after.views, DiffableUtils.getStringKeySerializer());
        }

        ViewMetadataDiff(StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), View::new, View::readDiffFrom);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ViewMetadata(dataStreamDiff.apply(((ViewMetadata) part).views));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
