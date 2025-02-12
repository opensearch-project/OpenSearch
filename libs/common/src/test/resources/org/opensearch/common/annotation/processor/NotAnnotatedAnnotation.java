/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target({
    ElementType.TYPE,
    ElementType.TYPE_PARAMETER,
    ElementType.TYPE_USE,
    ElementType.PACKAGE,
    ElementType.METHOD,
    ElementType.CONSTRUCTOR,
    ElementType.PARAMETER,
    ElementType.FIELD,
    ElementType.ANNOTATION_TYPE,
    ElementType.MODULE })
public @interface NotAnnotatedAnnotation {

}
