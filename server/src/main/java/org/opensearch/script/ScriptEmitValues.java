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

    // Emits a Long value  (ex. from a long or date field type)
    public static final class Long {

        private final DerivedFieldScript derivedFieldScript;

        public Long(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        public void emitLong(long val) {
            derivedFieldScript.addEmittedValue(val);
        }

    }

    // Emits a Double value
    public static final class Double {

        private final DerivedFieldScript derivedFieldScript;

        public Double(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        public void emitDouble(double val) {
            derivedFieldScript.addEmittedValue(val);
        }
    }

    // Emits a GeoPoint value
    public static final class GeoPoint {

        private final DerivedFieldScript derivedFieldScript;

        public GeoPoint(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        public void emitGeoPoint(double lat, double lon) {
            derivedFieldScript.addEmittedValue(new Tuple<>(lat, lon));
        }

    }

    // Emits a Boolean value
    public static final class Boolean {

        private final DerivedFieldScript derivedFieldScript;

        public Boolean(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        public void emitBoolean(boolean val) {
            derivedFieldScript.addEmittedValue(val);
        }
    }

    // Emits a String value
    public static final class Strings {

        private final DerivedFieldScript derivedFieldScript;

        public Strings(DerivedFieldScript derivedFieldScript) {
            this.derivedFieldScript = derivedFieldScript;
        }

        public void emitString(String val) {
            derivedFieldScript.addEmittedValue(val);
        }
    }
}
