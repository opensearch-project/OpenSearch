/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.script;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for a script context
*
* @opensearch.internal
*/
public class ProtobufScriptContextStats implements ProtobufWriteable, ToXContentFragment, Comparable<ProtobufScriptContextStats> {
    private final String context;
    private final long compilations;
    private final long cacheEvictions;
    private final long compilationLimitTriggered;

    public ProtobufScriptContextStats(String context, long compilations, long cacheEvictions, long compilationLimitTriggered) {
        this.context = Objects.requireNonNull(context);
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ProtobufScriptContextStats(CodedInputStream in) throws IOException {
        context = in.readString();
        compilations = in.readInt64();
        cacheEvictions = in.readInt64();
        compilationLimitTriggered = in.readInt64();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeStringNoTag(context);
        out.writeInt64NoTag(compilations);
        out.writeInt64NoTag(cacheEvictions);
        out.writeInt64NoTag(compilationLimitTriggered);
    }

    public String getContext() {
        return context;
    }

    public long getCompilations() {
        return compilations;
    }

    public long getCacheEvictions() {
        return cacheEvictions;
    }

    public long getCompilationLimitTriggered() {
        return compilationLimitTriggered;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.CONTEXT, getContext());
        builder.field(Fields.COMPILATIONS, getCompilations());
        builder.field(Fields.CACHE_EVICTIONS, getCacheEvictions());
        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, getCompilationLimitTriggered());
        builder.endObject();
        return builder;
    }

    @Override
    public int compareTo(ProtobufScriptContextStats o) {
        return this.context.compareTo(o.context);
    }

    static final class Fields {
        static final String CONTEXT = "context";
        static final String COMPILATIONS = "compilations";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
    }
}
