/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.PrettyPrinter;
import tools.jackson.core.io.SerializedString;
import tools.jackson.core.util.DefaultIndenter;
import tools.jackson.core.util.DefaultPrettyPrinter;

public class XObjectWriteContext extends ObjectWriteContext.Base {
    private static final XObjectWriteContext DEFAULT_INSTANCE = new XObjectWriteContext(false);

    private static final SerializedString LF = new SerializedString("\n");
    private static final DefaultPrettyPrinter.Indenter INDENTER = new DefaultIndenter("  ", LF.getValue());
    private final PrettyPrinter prettyPrinter;

    public static XObjectWriteContext create(boolean prettyPrint) {
        if (prettyPrint == true) {
            return new XObjectWriteContext(prettyPrint);
        } else {
            return DEFAULT_INSTANCE;
        }
    }

    private XObjectWriteContext(boolean prettyPrint) {
        this.prettyPrinter = prettyPrint ? new DefaultPrettyPrinter().withObjectIndenter(INDENTER).withArrayIndenter(INDENTER) : null;
    }

    @Override
    public PrettyPrinter getPrettyPrinter() {
        return prettyPrinter;
    }

    @Override
    public void writeValue(JsonGenerator g, Object value) {
        // Bringing the implementation from Jackson 2.x release line (see please
        // https://github.com/FasterXML/jackson-core/blob/2.x/src/main/java/com/fasterxml/jackson/core/JsonGenerator.java#L3099).

        // 31-Dec-2009, tatu: Actually, we could just handle some basic
        // types even without codec. This can improve interoperability,
        // and specifically help with TokenBuffer.
        if (value == null) {
            g.writeNull();
            return;
        }
        if (value instanceof String) {
            g.writeString((String) value);
            return;
        }
        if (value instanceof Number) {
            Number n = (Number) value;
            if (n instanceof Integer) {
                g.writeNumber(n.intValue());
                return;
            } else if (n instanceof Long) {
                g.writeNumber(n.longValue());
                return;
            } else if (n instanceof Double) {
                g.writeNumber(n.doubleValue());
                return;
            } else if (n instanceof Float) {
                g.writeNumber(n.floatValue());
                return;
            } else if (n instanceof Short) {
                g.writeNumber(n.shortValue());
                return;
            } else if (n instanceof Byte) {
                g.writeNumber(n.byteValue());
                return;
            } else if (n instanceof BigInteger) {
                g.writeNumber((BigInteger) n);
                return;
            } else if (n instanceof BigDecimal) {
                g.writeNumber((BigDecimal) n);
                return;
                // then Atomic types
            } else if (n instanceof AtomicInteger) {
                g.writeNumber(((AtomicInteger) n).get());
                return;
            } else if (n instanceof AtomicLong) {
                g.writeNumber(((AtomicLong) n).get());
                return;
            }
        } else if (value instanceof byte[]) {
            g.writeBinary((byte[]) value);
            return;
        } else if (value instanceof Boolean) {
            g.writeBoolean((Boolean) value);
            return;
        } else if (value instanceof AtomicBoolean) {
            g.writeBoolean(((AtomicBoolean) value).get());
            return;
        }
        throw new IllegalStateException(
            "No ObjectCodec defined for the generator, can only serialize simple wrapper types (type passed "
                + value.getClass().getName()
                + ")"
        );
    }
}
