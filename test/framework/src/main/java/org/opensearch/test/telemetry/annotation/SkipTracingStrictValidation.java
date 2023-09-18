/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to skip Tracing strict check in fatal scenarios like forced node drop etc. where spans can not even be closed
 * in the failure and exception scenarios. This annotation should really be used in the fatal scenarios only.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SkipTracingStrictValidation {
    /**
     * Users should define the reason for using this annotation.
     * @return reason for skipping the validation.
     */
    String reason();
}
