/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

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
import java.util.Map;
import java.util.Objects;

/**
 * A model class for Context metadata that can be used in metadata lib without
 * depending on the full Context class from server module.
 */
@ExperimentalApi
public final class ContextModel implements Writeable, ToXContentObject {

    /** ParseField for the context name. */
    public static final ParseField NAME_FIELD = new ParseField("name");
    /** ParseField for the context version. */
    public static final ParseField VERSION_FIELD = new ParseField("version");
    /** ParseField for the context parameters. */
    public static final ParseField PARAMS_FIELD = new ParseField("params");

    /** Default version when none is specified. */
    public static final String LATEST_VERSION = "_latest";

    /** Parser for ContextModel from XContent. */
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ContextModel, Void> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        a -> new ContextModel((String) a[0], (String) a[1], (Map<String, Object>) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), VERSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), PARAMS_FIELD);
    }

    private final String name;
    private final String version;
    private final Map<String, Object> params;

    /**
     * Creates a ContextModel with the given fields.
     * Null version is defaulted to {@link #LATEST_VERSION}.
     *
     * @param name the context name
     * @param version the context version (null defaults to {@link #LATEST_VERSION})
     * @param params the context parameters
     */
    public ContextModel(String name, String version, Map<String, Object> params) {
        this.name = name;
        this.version = version != null ? version : LATEST_VERSION;
        this.params = params;
    }

    /**
     * Creates a ContextModel by reading from StreamInput.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public ContextModel(StreamInput in) throws IOException {
        this.name = in.readString();
        this.version = in.readOptionalString();
        this.params = in.readMap();
    }

    /**
     * Writes to StreamOutput.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(version);
        out.writeMap(params);
    }

    /**
     * Returns the context name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the context version.
     *
     * @return the version
     */
    public String version() {
        return version;
    }

    /**
     * Returns the context parameters.
     *
     * @return the params map
     */
    public Map<String, Object> params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContextModel that = (ContextModel) o;
        return Objects.equals(name, that.name) && Objects.equals(version, that.version) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version, params);
    }

    /**
     * Parses a ContextModel from XContent.
     *
     * @param parser the XContent parser
     * @return the parsed ContextModel
     */
    public static ContextModel fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Writes this ContextModel to XContent.
     *
     * @param builder the XContent builder
     * @param params the ToXContent params
     * @return the XContent builder
     * @throws IOException if writing fails
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        if (this.params != null) {
            builder.field(PARAMS_FIELD.getPreferredName(), this.params);
        }
        builder.endObject();
        return builder;
    }
}
