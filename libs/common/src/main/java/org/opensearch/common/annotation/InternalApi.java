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
 * Internal APIs that have no compatibility guarantees and should be not used outside
 * of OpenSearch core components.
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
public @interface InternalApi {

}
