/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test logs appender that provides functionality to extract specific logs/exception messages and wait for it to show up
 * @opensearch.internal
 */
public class TestLogsAppender extends AbstractAppender {
    private final List<String> capturedLogs = new ArrayList<>();
    private final List<String> messagesToCapture;

    public TestLogsAppender(List<String> messagesToCapture) {
        super("TestAppender", null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
        this.messagesToCapture = messagesToCapture;
        start();
    }

    @Override
    public void append(LogEvent event) {
        if (shouldCaptureMessage(event.getMessage().getFormattedMessage())) capturedLogs.add(event.getMessage().getFormattedMessage());
        if (event.getThrown() != null) {
            if (shouldCaptureMessage(event.getThrown().toString())) capturedLogs.add(event.getThrown().toString());
            for (StackTraceElement element : event.getThrown().getStackTrace())
                if (shouldCaptureMessage(element.toString())) capturedLogs.add(element.toString());
        }
    }

    public boolean shouldCaptureMessage(String log) {
        return messagesToCapture.stream().anyMatch(log::contains);
    }

    public List<String> getCapturedLogs() {
        return new ArrayList<>(capturedLogs);
    }

    public boolean waitForLog(String expectedLog, long timeout, TimeUnit unit) {
        long startTime = System.currentTimeMillis();
        long timeoutInMillis = unit.toMillis(timeout);

        while (System.currentTimeMillis() - startTime < timeoutInMillis) {
            if (capturedLogs.stream().anyMatch(log -> log.contains(expectedLog))) {
                return true;
            }
            try {
                Thread.sleep(100); // Wait for 100ms before checking again
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return false;
    }

    // Clear captured logs
    public void clearCapturedLogs() {
        capturedLogs.clear();
    }
}
