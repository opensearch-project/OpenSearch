
package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

@ExperimentalApi
public class View extends AbstractDiffable<View> implements ToXContentObject {

    public final String name;
    public final String description;
    public final long createdAt;
    public final long modifiedAt;
    public final List<Target> targets;

    public View(final String name, final String description, final long createdAt, final long modifiedAt, final List<Target> targets) {
        this.name = name;
        this.description = description;
        this.createdAt = createdAt;
        this.modifiedAt = modifiedAt;
        this.targets = targets;
    }

    public View(final StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString(), in.readVLong(), in.readVLong(), in.readList(Target::new));
    }

    public static Diff<View> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(View::new, in);
    }

    public static class Target implements Writeable, ToXContentObject {

        public final String indexPattern;

        private Target(final String indexPattern) {
            this.indexPattern = indexPattern;
        }

        private Target(final StreamInput in) throws IOException {
            this(in.readString());
        }

        private static final ParseField INDEX_PATTERN_FIELD = new ParseField("indexPattern");

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_PATTERN_FIELD.getPreferredName(), indexPattern);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Target, Void> T_PARSER = new ConstructingObjectParser<>(
            "target",
            args -> new Target((String) args[0])
        );
        static {
            T_PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_PATTERN_FIELD);
        }

        public static Target fromXContent(final XContentParser parser) throws IOException {
            return T_PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexPattern);
        }
    }

    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    private static final ParseField CREATED_AT_FIELD = new ParseField("createdAt");
    private static final ParseField MODIFIED_AT_FIELD = new ParseField("modifiedAt");
    private static final ParseField TARGETS_FIELD = new ParseField("targets");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<View, Void> PARSER = new ConstructingObjectParser<>(
        "view",
        args -> new View((String) args[0], (String) args[1], (Long) args[2], (Long) args[3], (List<Target>) args[4])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION_FIELD);
        PARSER.declareLongOrNull(ConstructingObjectParser.optionalConstructorArg(), -1L, CREATED_AT_FIELD);
        PARSER.declareLongOrNull(ConstructingObjectParser.optionalConstructorArg(), -1L, MODIFIED_AT_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Target.fromXContent(p), TARGETS_FIELD);
    }

    public static View fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(DESCRIPTION_FIELD.getPreferredName(), description);
        builder.field(CREATED_AT_FIELD.getPreferredName(), createdAt);
        builder.field(MODIFIED_AT_FIELD.getPreferredName(), modifiedAt);
        builder.field(TARGETS_FIELD.getPreferredName(), targets);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(description);
        out.writeVLong(createdAt);
        out.writeVLong(modifiedAt);
        out.writeList(targets);
    }
}
