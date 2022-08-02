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
 * A class binding represents a method call that stores state. Each class binding's Java class must
 * have exactly one public constructor and one public method excluding those inherited directly
 * from {@link Object}. The canonical type name parameters provided must match those of the
 * constructor and method combined. The constructor for a class binding's Java class will be called
 * when the binding method is called for the first time at which point state may be stored for the
 * arguments passed into the constructor. The method for a binding class will be called each time
 * the binding method is called and may use the previously stored state.
 */
public class AllowlistClassBinding extends WhitelistClassBinding {
    /**
     * Standard constructor. All values must be not {@code null}.
     */
    public AllowlistClassBinding(
        String origin,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        List<Object> painlessAnnotations
    ) {
        super(origin, targetJavaClassName, methodName, returnCanonicalTypeName, canonicalTypeNameParameters, painlessAnnotations);
    }
}
