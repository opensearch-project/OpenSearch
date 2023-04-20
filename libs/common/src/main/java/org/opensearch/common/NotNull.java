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
import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierNickname;

/**
 * The presence of this annotation on a method indicates that
 * {@code null} will not be returned from that method.
 *
 * @opensearch.api
 */
@Documented
@TypeQualifierNickname
@Nonnull
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD })
public @interface NotNull {
}

