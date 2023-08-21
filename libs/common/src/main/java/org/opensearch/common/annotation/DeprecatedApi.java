/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Marks the public APIs as deprecated and scheduled for removal in one of the upcoming
 * major releases. The types marked with this annotations could only be other {@link PublicApi}s.
 *
 * @opensearch.api
 */
@Documented
@Target({
    ElementType.TYPE,
    ElementType.PACKAGE,
    ElementType.METHOD,
    ElementType.CONSTRUCTOR,
    ElementType.PARAMETER,
    ElementType.FIELD,
    ElementType.ANNOTATION_TYPE,
    ElementType.MODULE })
@PublicApi(since = "2.10.0")
public @interface DeprecatedApi {
    /**
     * Version since this API is deprecated
     */
    String since();

    /**
     * Next major version when this API is scheduled for removal
     */
    String forRemoval() default "";
}
