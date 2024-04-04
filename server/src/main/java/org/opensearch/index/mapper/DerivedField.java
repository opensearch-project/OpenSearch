/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.script.Script;

import java.io.IOException;

/**
 * DerivedField representation: expects a name, type and script.
 */
@PublicApi(since = "2.14.0")
public class DerivedField implements Writeable, ToXContentFragment {

    private final String name;
    private final String type;
    private final Script script;

    public DerivedField(String name, String type, Script script) {
        this.name = name;
        this.type = type;
        this.script = script;
    }

    public DerivedField(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        script = new Script(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        script.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(name);
        builder.field(type);
        builder.field("script", script);
        builder.endObject();
        return builder;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Script getScript() {
        return script;
    }

}
