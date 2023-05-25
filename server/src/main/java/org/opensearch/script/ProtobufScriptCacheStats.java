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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Stats for script caching
*
* @opensearch.internal
*
* @deprecated This class is deprecated in favor of ProtobufScriptStats and ScriptContextStats.  It is removed in OpenSearch 2.0.
*/
@Deprecated
public class ProtobufScriptCacheStats implements ProtobufWriteable, ToXContentFragment {
    private final Map<String, ProtobufScriptStats> context;
    private final ProtobufScriptStats general;

    public ProtobufScriptCacheStats(Map<String, ProtobufScriptStats> context) {
        this.context = Collections.unmodifiableMap(context);
        this.general = null;
    }

    public ProtobufScriptCacheStats(ProtobufScriptStats general) {
        this.general = Objects.requireNonNull(general);
        this.context = null;
    }

    public ProtobufScriptCacheStats(CodedInputStream in) throws IOException {
        boolean isContext = in.readBool();
        if (isContext == false) {
            general = new ProtobufScriptStats(in);
            context = null;
            return;
        }

        general = null;
        int size = in.readInt32();
        Map<String, ProtobufScriptStats> context = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            context.put(name, new ProtobufScriptStats(in));
        }
        this.context = Collections.unmodifiableMap(context);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        if (general != null) {
            out.writeBoolNoTag(false);
            general.writeTo(out);
            return;
        }

        out.writeBoolNoTag(true);
        out.writeInt32NoTag(context.size());
        for (String name : context.keySet().stream().sorted().collect(Collectors.toList())) {
            out.writeStringNoTag(name);
            context.get(name).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_CACHE_STATS);
        builder.startObject(Fields.SUM);
        if (general != null) {
            builder.field(ProtobufScriptStats.Fields.COMPILATIONS, general.getCompilations());
            builder.field(ProtobufScriptStats.Fields.CACHE_EVICTIONS, general.getCacheEvictions());
            builder.field(ProtobufScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, general.getCompilationLimitTriggered());
            builder.endObject().endObject();
            return builder;
        }

        ProtobufScriptStats sum = sum();
        builder.field(ProtobufScriptStats.Fields.COMPILATIONS, sum.getCompilations());
        builder.field(ProtobufScriptStats.Fields.CACHE_EVICTIONS, sum.getCacheEvictions());
        builder.field(ProtobufScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, sum.getCompilationLimitTriggered());
        builder.endObject();

        builder.startArray(Fields.CONTEXTS);
        for (String name : context.keySet().stream().sorted().collect(Collectors.toList())) {
            ProtobufScriptStats stats = context.get(name);
            builder.startObject();
            builder.field(Fields.CONTEXT, name);
            builder.field(ProtobufScriptStats.Fields.COMPILATIONS, stats.getCompilations());
            builder.field(ProtobufScriptStats.Fields.CACHE_EVICTIONS, stats.getCacheEvictions());
            builder.field(ProtobufScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, stats.getCompilationLimitTriggered());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();

        return builder;
    }

    /**
     * Get the context specific stats, null if using general cache
    */
    public Map<String, ProtobufScriptStats> getContextStats() {
        return context;
    }

    /**
     * Get the general stats, null if using context cache
    */
    public ProtobufScriptStats getGeneralStats() {
        return general;
    }

    /**
     * The sum of all script stats, either the general stats or the sum of all stats of the context stats.
    */
    public ProtobufScriptStats sum() {
        if (general != null) {
            return general;
        }
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ProtobufScriptStats stat : context.values()) {
            compilations += stat.getCompilations();
            cacheEvictions += stat.getCacheEvictions();
            compilationLimitTriggered += stat.getCompilationLimitTriggered();
        }
        return new ProtobufScriptStats(compilations, cacheEvictions, compilationLimitTriggered);
    }

    static final class Fields {
        static final String SCRIPT_CACHE_STATS = "script_cache";
        static final String CONTEXT = "context";
        static final String SUM = "sum";
        static final String CONTEXTS = "contexts";
    }
}
