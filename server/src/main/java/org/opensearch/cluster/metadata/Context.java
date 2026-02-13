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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.index.model.ContextModel;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Class encapsulating the context metadata associated with an index template/index.
 */
@ExperimentalApi
public class Context extends AbstractDiffable<Context> implements ToXContentObject {

    public static final String LATEST_VERSION = ContextModel.LATEST_VERSION;

    private final ContextModel model;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Context, Void> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        a -> new Context((String) a[0], (String) a[1], (Map<String, Object>) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ContextModel.NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ContextModel.VERSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), ContextModel.PARAMS_FIELD);
    }

    public Context(String name) {
        this.model = new ContextModel(name, null, Map.of());
    }

    public Context(String name, String version, Map<String, Object> params) {
        this.model = new ContextModel(name, version, params);
    }

    public Context(StreamInput in) throws IOException {
        this.model = new ContextModel(in);
    }

    public Context(ContextModel model) {
        this.model = model;
    }

    public String name() {
        return model.name();
    }

    public String version() {
        return model.version();
    }

    public Map<String, Object> params() {
        return model.params();
    }

    public ContextModel model() {
        return model;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        model.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return model.toXContent(builder, params);
    }

    public static Context fromXContent(XContentParser parser) {
        return new Context(ContextModel.fromXContent(parser));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Context context = (Context) o;
        return Objects.equals(model, context.model);
    }

    @Override
    public int hashCode() {
        return Objects.hash(model);
    }

    @Override
    public String toString() {
        return "Context{" + "name='" + model.name() + '\'' + ", version='" + model.version() + '\'' + ", params=" + model.params() + '}';
    }
}
