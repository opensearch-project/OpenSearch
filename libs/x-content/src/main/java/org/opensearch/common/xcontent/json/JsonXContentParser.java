/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.xcontent.json;

import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.AbstractXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.tools.jackson.core.JacksonExceptionTranslator;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.TokenStreamLocation;

public class JsonXContentParser extends AbstractXContentParser {

    final JsonParser parser;

    public JsonXContentParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler);
        this.parser = parser;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public Token nextToken() throws IOException {
        try {
            return convertToken(parser.nextToken());
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public void skipChildren() throws IOException {
        try {
            parser.skipChildren();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public Token currentToken() {
        return convertToken(parser.currentToken());
    }

    @Override
    public NumberType numberType() throws IOException {
        try {
            return convertNumberType(parser.getNumberType());
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public String currentName() throws IOException {
        return parser.currentName();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        try {
            return parser.getBooleanValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public String text() throws IOException {
        if (currentToken().isValue()) {
            try {
                return parser.getString();
            } catch (final JacksonException ex) {
                JacksonExceptionTranslator.translateOrRethrow(ex);
            }
        }
        throw new IllegalStateException("Can't get text on a " + currentToken() + " at " + getTokenLocation());
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        try {
            return CharBuffer.wrap(parser.getStringCharacters(), parser.getStringOffset(), parser.getStringLength());
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public Object objectText() throws IOException {
        try {
            JsonToken currentToken = parser.currentToken();
            if (currentToken == JsonToken.VALUE_STRING) {
                return text();
            } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
                return parser.getNumberValue();
            } else if (currentToken == JsonToken.VALUE_TRUE) {
                return Boolean.TRUE;
            } else if (currentToken == JsonToken.VALUE_FALSE) {
                return Boolean.FALSE;
            } else if (currentToken == JsonToken.VALUE_NULL) {
                return null;
            } else {
                return text();
            }
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public Object objectBytes() throws IOException {
        try {
            JsonToken currentToken = parser.currentToken();
            if (currentToken == JsonToken.VALUE_STRING) {
                return charBuffer();
            } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
                return parser.getNumberValue();
            } else if (currentToken == JsonToken.VALUE_TRUE) {
                return Boolean.TRUE;
            } else if (currentToken == JsonToken.VALUE_FALSE) {
                return Boolean.FALSE;
            } else if (currentToken == JsonToken.VALUE_NULL) {
                return null;
            } else {
                return charBuffer();
            }
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public boolean hasTextCharacters() {
        return parser.hasStringCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        try {
            return parser.getStringCharacters();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public int textLength() throws IOException {
        try {
            return parser.getStringLength();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public int textOffset() throws IOException {
        try {
            return parser.getStringOffset();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public Number numberValue() throws IOException {
        try {
            return parser.getNumberValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public short doShortValue() throws IOException {
        try {
            return parser.getShortValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public int doIntValue() throws IOException {
        try {
            return parser.getIntValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public long doLongValue() throws IOException {
        try {
            return parser.getLongValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public float doFloatValue() throws IOException {
        try {
            return parser.getFloatValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public double doDoubleValue() throws IOException {
        try {
            return parser.getDoubleValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public BigInteger doBigIntegerValue() throws IOException {
        try {
            if (parser.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
                return parser.getDecimalValue().toBigInteger();
            } else {
                return parser.getBigIntegerValue();
            }
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public byte[] binaryValue() throws IOException {
        try {
            return parser.getBinaryValue();
        } catch (final JacksonException ex) {
            throw JacksonExceptionTranslator.translateOrRethrowReturning(ex);
        }
    }

    @Override
    public XContentLocation getTokenLocation() {
        TokenStreamLocation loc = parser.currentTokenLocation();
        if (loc == null) {
            return null;
        }
        return new XContentLocation(loc.getLineNr(), loc.getColumnNr());
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(parser);
    }

    private NumberType convertNumberType(JsonParser.NumberType numberType) {
        switch (numberType) {
            case INT:
                return NumberType.INT;
            case BIG_INTEGER:
                return NumberType.BIG_INTEGER;
            case LONG:
                return NumberType.LONG;
            case FLOAT:
                return NumberType.FLOAT;
            case DOUBLE:
                return NumberType.DOUBLE;
            case BIG_DECIMAL:
                return NumberType.BIG_DECIMAL;
        }
        throw new IllegalStateException("No matching token for number_type [" + numberType + "]");
    }

    private Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        switch (token) {
            case PROPERTY_NAME:
                return Token.FIELD_NAME;
            case VALUE_FALSE:
            case VALUE_TRUE:
                return Token.VALUE_BOOLEAN;
            case VALUE_STRING:
                return Token.VALUE_STRING;
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return Token.VALUE_NUMBER;
            case VALUE_NULL:
                return Token.VALUE_NULL;
            case START_OBJECT:
                return Token.START_OBJECT;
            case END_OBJECT:
                return Token.END_OBJECT;
            case START_ARRAY:
                return Token.START_ARRAY;
            case END_ARRAY:
                return Token.END_ARRAY;
            case VALUE_EMBEDDED_OBJECT:
                return Token.VALUE_EMBEDDED_OBJECT;
        }
        throw new IllegalStateException("No matching token for json_token [" + token + "]");
    }

    @Override
    public boolean isClosed() {
        return parser.isClosed();
    }
}
