/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import java.io.Serializable;

/**
 * Extension of {@link AbstractAppender} that provides a no-op stop()
 * implementation to avoid interference issues between tests running
 * concurrently.
 */
public abstract class AbstractTestAppender extends AbstractAppender {

    protected AbstractTestAppender(
        String name,
        Filter filter,
        Layout<? extends Serializable> layout,
        boolean ignoreExceptions,
        Property[] properties
    ) {
        super(name, filter, layout, ignoreExceptions, properties);
    }

    @Override
    public void stop() {
        // Do nothing. This feels wrong but there is nothing for this logger
        // to clean up or flush, and calling the parent stop() will change its
        // lifecycle state which can lead to race conditions in the static
        // logger state where in flight logger messages try to append to a
        // stopped appender and cause spurious test failures.
    }
}
