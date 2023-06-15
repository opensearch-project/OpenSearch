/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.exporter;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.logging.log4j.message.ParameterizedMessage;

public class MockAppender extends AbstractAppender {
    public LogEvent lastEvent;

    public MockAppender(final String name) throws IllegalAccessException {
        super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
    }

    @Override
    public void append(LogEvent event) {
        lastEvent = event.toImmutable();
    }

    ParameterizedMessage lastParameterizedMessage() {
        return (ParameterizedMessage) lastEvent.getMessage();
    }

    public LogEvent getLastEventAndReset() {
        LogEvent toReturn = lastEvent;
        lastEvent = null;
        return toReturn;
    }
}
