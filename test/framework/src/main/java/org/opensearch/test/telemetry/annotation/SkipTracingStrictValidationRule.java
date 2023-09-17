/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.annotation;

import org.opensearch.test.telemetry.tracing.StrictCheckSpanProcessor;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Define the rule for Skipping Trace validation.
 */
public class SkipTracingStrictValidationRule extends TestWatcher {

    @Override
    public void starting(Description description) {
        SkipTracingStrictValidation annotation = description.getAnnotation(SkipTracingStrictValidation.class);
        if (annotation != null) {
            StrictCheckSpanProcessor.pauseStrictValidation();
        }
    }

    @Override
    public void finished(Description description) {
        SkipTracingStrictValidation annotation = description.getAnnotation(SkipTracingStrictValidation.class);
        if (annotation != null) {
            StrictCheckSpanProcessor.resumeStrictValidation();
        }
    }

}
