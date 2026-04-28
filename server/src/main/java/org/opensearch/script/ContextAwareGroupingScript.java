/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.ContextAwareGroupingFieldMapper;

import java.util.Map;

/**
 * A script used in {@link ContextAwareGroupingFieldMapper} which acts as grouping criteria function
 * for context aware segments
 *
 * @opensearch.internal
 */
public abstract class ContextAwareGroupingScript {
    public static final String[] PARAMETERS = { "ctx" };

    /** The context used to compile {@link ContextAwareGroupingScript} factories. */
    public static final ScriptContext<ContextAwareGroupingScript.Factory> CONTEXT = new ScriptContext<>(
        ContextAwareGroupingFieldMapper.CONTENT_TYPE,
        ContextAwareGroupingScript.Factory.class,
        200,
        TimeValue.timeValueMillis(10000),
        ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple()
    );

    public abstract String execute(Map<String, Object> ctx);

    /**
     * A factory to construct stateful {@link ContextAwareGroupingScript} factories for a specific index.
     *
     * @opensearch.internal
     */
    public interface Factory {
        ContextAwareGroupingScript newInstance();
    }
}
