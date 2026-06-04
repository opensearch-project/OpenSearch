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
 * Experimental APIs that may not retain source and binary compatibility within major,
 * minor or patch releases. The types marked with this annotations could only expose
 * other {@link PublicApi} or {@link ExperimentalApi} types as public members.
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
public @interface ExperimentalApi {

}
