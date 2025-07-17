/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Class encapsulating the context metadata associated with an index template/index.
 */
@ExperimentalApi
public class Context extends AbstractDiffable<Context> implements ToXContentObject {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField PARAMS = new ParseField("params");

    public static final String LATEST_VERSION = "_latest";

    private String name;
    private String version = LATEST_VERSION;
    private Map<String, Object> params;

    public static final ConstructingObjectParser<Context, Void> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        a -> new Context((String) a[0], (String) a[1], (Map<String, Object>) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), PARAMS);
    }

    public Context(String name) {
        this(name, LATEST_VERSION, Map.of());
    }

    public Context(String name, String version, Map<String, Object> params) {
        this.name = name;
        if (version != null) {
            this.version = version;
        }
        this.params = params;
    }

    public Context(StreamInput in) throws IOException {
        this.name = in.readString();
        this.version = in.readOptionalString();
        this.params = in.readMap();
    }

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String version() {
        return version;
    }

    public void version(String version) {
        this.version = version;
    }

    public Map<String, Object> params() {
        return params;
    }

    public void params(Map<String, Object> params) {
        this.params = params;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(version);
        out.writeMap(params);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), this.name);
        builder.field(VERSION.getPreferredName(), this.version);
        if (this.params != null) {
            builder.field(PARAMS.getPreferredName(), this.params);
        }
        builder.endObject();
        return builder;
    }

    public static Context fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Context context = (Context) o;
        return Objects.equals(name, context.name) && Objects.equals(version, context.version) && Objects.equals(params, context.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version, params);
    }

    @Override
    public String toString() {
        return "Context{" + "name='" + name + '\'' + ", version='" + version + '\'' + ", params=" + params + '}';
    }
}
