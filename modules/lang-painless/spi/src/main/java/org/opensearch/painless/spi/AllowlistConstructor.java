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
 * Constructor represents the equivalent of a Java constructor available as a allowlisted class
 * constructor within Painless. Constructors for Painless classes may be accessed exactly as
 * constructors for Java classes are using the 'new' keyword. Painless classes may have multiple
 * constructors as long as they comply with arity overloading described for {@link WhitelistClass}.
 */
public final class AllowlistConstructor extends WhitelistConstructor {
    /**
     * Standard constructor. All values must be not {@code null}.
     */
    AllowlistConstructor(String origin, List<String> canonicalTypeNameParameters, List<Object> painlessAnnotations) {
        super(origin, canonicalTypeNameParameters, painlessAnnotations);
    }
}
