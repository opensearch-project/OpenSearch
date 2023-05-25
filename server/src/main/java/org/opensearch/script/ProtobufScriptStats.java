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
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stats for scripts
*
* @opensearch.internal
*/
public class ProtobufScriptStats implements ProtobufWriteable, ToXContentFragment {
    private final List<ProtobufScriptContextStats> contextStats;
    private final long compilations;
    private final long cacheEvictions;
    private final long compilationLimitTriggered;

    public ProtobufScriptStats(List<ProtobufScriptContextStats> contextStats) {
        ArrayList<ProtobufScriptContextStats> ctxStats = new ArrayList<>(contextStats.size());
        ctxStats.addAll(contextStats);
        ctxStats.sort(ProtobufScriptContextStats::compareTo);
        this.contextStats = Collections.unmodifiableList(ctxStats);
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ProtobufScriptContextStats stats : contextStats) {
            compilations += stats.getCompilations();
            cacheEvictions += stats.getCacheEvictions();
            compilationLimitTriggered += stats.getCompilationLimitTriggered();
        }
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ProtobufScriptStats(long compilations, long cacheEvictions, long compilationLimitTriggered) {
        this.contextStats = Collections.emptyList();
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ProtobufScriptStats(ProtobufScriptContextStats context) {
        this(context.getCompilations(), context.getCacheEvictions(), context.getCompilationLimitTriggered());
    }

    public ProtobufScriptStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        compilations = in.readInt64();
        cacheEvictions = in.readInt64();
        compilationLimitTriggered = in.readInt64();
        contextStats = protobufStreamInput.readList(ProtobufScriptContextStats::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt64NoTag(compilations);
        out.writeInt64NoTag(cacheEvictions);
        out.writeInt64NoTag(compilationLimitTriggered);
        protobufStreamOutput.writeCollection(contextStats, (o, v) -> v.writeTo(o));
    }

    public List<ProtobufScriptContextStats> getContextStats() {
        return contextStats;
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

    public ProtobufScriptCacheStats toScriptCacheStats() {
        if (contextStats.isEmpty()) {
            return new ProtobufScriptCacheStats(this);
        }
        Map<String, ProtobufScriptStats> contexts = new HashMap<>(contextStats.size());
        for (ProtobufScriptContextStats contextStats : contextStats) {
            contexts.put(contextStats.getContext(), new ProtobufScriptStats(contextStats));
        }
        return new ProtobufScriptCacheStats(contexts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_STATS);
        builder.field(Fields.COMPILATIONS, compilations);
        builder.field(Fields.CACHE_EVICTIONS, cacheEvictions);
        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, compilationLimitTriggered);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String SCRIPT_STATS = "script";
        static final String CONTEXTS = "contexts";
        static final String COMPILATIONS = "compilations";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
    }
}
