/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.opensearch.common.collect.Tuple;

/**
 * Values that can be emitted in a derived field script context.
 * <p>
 * The emit function can be called multiple times within a script definition
 * so the function will handle collecting the values over the script execution.
 */
public final class ScriptEmitValues {

    /**
     * Takes in a single value and emits it
     * Could be a long, double, String, etc.
     */
    public static final class EmitSingle {

        private final DerivedFieldScript derivedFieldScript;

        public EmitSingle(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        // TODO: Keeping this generic for the time being due to limitations with
        // binding methods with the same name and arity.
        // Ideally, we should have an emit signature per derived field type and try to scope
        // that to the respective script execution so the other emits aren't allowed.
        // One way to do this could be to create implementations of the DerivedFieldScript.LeafFactory
        // per field type where they each define their own emit() method and then the engine that executes
        // it can have custom compilation logic to perform class bindings on that emit implementation.
        public void emit(Object val) {
            derivedFieldScript.addEmittedValue(val);
        }

    }

    /**
     * Emits a GeoPoint value
     */
    public static final class GeoPoint {

        private final DerivedFieldScript derivedFieldScript;

        public GeoPoint(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        public void emit(double lat, double lon) {
            derivedFieldScript.addEmittedValue(new Tuple<>(lat, lon));
        }

    }

}
