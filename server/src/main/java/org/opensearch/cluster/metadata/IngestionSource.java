/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;


/**
 * Class encapsulating the configuration of an ingestion source.
 */
@ExperimentalApi
public class IngestionSource implements ToXContentObject  {
    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField PARAMS = new ParseField("params");

    private String type;
    private Map<String, Object> params;

    public static final ConstructingObjectParser<IngestionSource, Void> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        a -> new IngestionSource((String) a[0], (Map<String, Object>) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), PARAMS);
    }

    public IngestionSource(String type) {
        this(type, Map.of());
    }

    public IngestionSource(String type, Map<String, Object> params) {
        this.type = type;
        this.params = params;
    }

    public IngestionSource(StreamInput in) throws IOException {
        type = in.readString();
        params = in.readMap();
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> params() {
        return params;
    }

    public void params(Map<String, Object> params) {
        this.params = params;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), this.type);
        if (this.params != null) {
            builder.field(PARAMS.getPreferredName(), this.params);
        }
        builder.endObject();
        return builder;
    }

    public static IngestionSource fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestionSource ingestionSource = (IngestionSource) o;
        return Objects.equals(type, ingestionSource.type) && Objects.equals(params, ingestionSource.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, params);
    }

    @Override
    public String toString() {
        return "IngestionSource{" + "type='" + type + '\'' + ", params=" + params + '}';
    }
}
