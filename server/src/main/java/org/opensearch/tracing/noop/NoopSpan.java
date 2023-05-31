/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.noop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.tracing.AbstractSpan;
import org.opensearch.tracing.Level;
import org.opensearch.tracing.Span;

/**
 * No-op implementation of Span
 */
public class NoopSpan extends AbstractSpan {

    private static final Logger logger = LogManager.getLogger(NoopSpan.class);

    public NoopSpan(String spanName, Span parentSpan, Level level) {
        super(spanName, parentSpan, level);
        logger.trace("Starting Noop span name:{}", spanName);
    }

    @Override
    public void endSpan() {
        logger.trace("Ending noop span name:{}", spanName);
    }

    @Override
    public void addAttribute(String key, String value) {
        logger.warn("Adding attribute {}:{} in no op span, will be ignored", key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        logger.warn("Adding attribute {}:{} in no op span, will be ignored", key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        logger.warn("Adding attribute {}:{} in no op span, will be ignored", key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        logger.warn("Adding attribute {}:{} in no op span, will be ignored", key, value);
    }

    @Override
    public void addEvent(String event) {
        logger.warn("Adding event {} in no op span, will be ignored", event);
    }

    @Override
    public String getTraceId() {
        return null;
    }

    @Override
    public String getSpanId() {
        return null;
    }

}
