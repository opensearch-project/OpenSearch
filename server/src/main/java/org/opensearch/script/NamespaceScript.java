package org.opensearch.script;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.NamespaceFieldMapper;

import java.util.Map;

public abstract class NamespaceScript {
    public static final String[] PARAMETERS = {"ctx"};

    public static final ScriptContext<NamespaceScript.Factory> CONTEXT = new ScriptContext<>(
        NamespaceFieldMapper.CONTENT_TYPE,
        NamespaceScript.Factory.class,
        200,
        TimeValue.timeValueMillis(10000),
        ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple());

    public abstract String execute(Map<String, Object> ctx);

    public interface Factory {
        NamespaceScript newInstance();
    }
}

