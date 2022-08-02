/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless.spi;

import java.util.List;

/**
 * Field represents the equivalent of a Java field available as an allowlisted class field
 * within Painless. Fields for Painless classes may be accessed exactly as fields for Java classes
 * are using the '.' operator on an existing class variable/field.
 */
public class AllowlistField extends WhitelistField {
    /**
     * Standard constructor.  All values must be not {@code null}.
     */
    public AllowlistField(String origin, String fieldName, String canonicalTypeNameParameter, List<Object> painlessAnnotations) {
        super(origin, fieldName, canonicalTypeNameParameter, painlessAnnotations);
    }
}
