/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.CheckForNull;
import javax.annotation.meta.TypeQualifierNickname;

/**
 * The presence of this annotation on a method parameter indicates that
 * {@code null} is *not* an acceptable value for that parameter.  It should not be
 * used for parameters of primitive types.
 *
 * @opensearch.api
 */
@Documented
@TypeQualifierNickname
@CheckForNull
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER, ElementType.FIELD, ElementType.METHOD })
public @interface Nonnull {
}
