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

package org.opensearch.core.xcontent;

import org.opensearch.common.Booleans;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Numbers;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Base class for parsing serializable content
 *
 * @opensearch.internal
 */
public abstract class AbstractXContentParser implements XContentParser {

    // Currently this is not a setting that can be changed and is a policy
    // that relates to how parsing of things like "boost" are done across
    // the whole of Elasticsearch (eg if String "1.0" is a valid float).
    // The idea behind keeping it as a constant is that we can track
    // references to this policy decision throughout the codebase and find
    // and change any code that needs to apply an alternative policy.
    public static final boolean DEFAULT_NUMBER_COERCE_POLICY = true;

    private static void checkCoerceString(boolean coerce, Class<? extends Number> clazz) {
        if (!coerce) {
            // Need to throw type IllegalArgumentException as current catch logic in
            // NumberFieldMapper.parseCreateField relies on this for "malformed" value detection
            throw new IllegalArgumentException(clazz.getSimpleName() + " value passed as String");
        }
    }

    private final NamedXContentRegistry xContentRegistry;
    private final DeprecationHandler deprecationHandler;

    public AbstractXContentParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler) {
        this.xContentRegistry = xContentRegistry;
        this.deprecationHandler = deprecationHandler;
    }

    // The 3rd party parsers we rely on are known to silently truncate fractions: see
    // http://fasterxml.github.io/jackson-core/javadoc/2.3.0/com/fasterxml/jackson/core/JsonParser.html#getShortValue()
    // If this behaviour is flagged as undesirable and any truncation occurs
    // then this method is called to trigger the"malformed" handling logic
    void ensureNumberConversion(boolean coerce, long result, Class<? extends Number> clazz) throws IOException {
        if (!coerce) {
            double fullVal = doDoubleValue();
            if (result != fullVal) {
                // Need to throw type IllegalArgumentException as current catch
                // logic in NumberFieldMapper.parseCreateField relies on this
                // for "malformed" value detection
                throw new IllegalArgumentException(fullVal + " cannot be converted to " + clazz.getSimpleName() + " without data loss");
            }
        }
    }

    void ensureNumberConversion(boolean coerce, BigInteger result, Class<? extends Number> clazz) throws IOException {
        if (!coerce) {
            double fullVal = doDoubleValue();
            if (result.doubleValue() != fullVal) {
                // Need to throw type IllegalArgumentException as current catch
                // logic in NumberFieldMapper.parseCreateField relies on this
                // for "malformed" value detection
                throw new IllegalArgumentException(fullVal + " cannot be converted to " + clazz.getSimpleName() + " without data loss");
            }
        }
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        switch (currentToken()) {
            case VALUE_BOOLEAN:
                return true;
            case VALUE_STRING:
                return Booleans.isBoolean(textCharacters(), textOffset(), textLength());
            default:
                return false;
        }
    }

    @Override
    public boolean booleanValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Booleans.parseBoolean(textCharacters(), textOffset(), textLength(), false /* irrelevant */);
        }
        return doBooleanValue();
    }

    protected abstract boolean doBooleanValue() throws IOException;

    @Override
    public short shortValue() throws IOException {
        return shortValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Short.class);

            double doubleValue = Double.parseDouble(text());

            if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + text() + "] is out of range for a short");
            }

            return (short) doubleValue;
        }
        short result = doShortValue();
        ensureNumberConversion(coerce, result, Short.class);
        return result;
    }

    protected abstract short doShortValue() throws IOException;

    @Override
    public int intValue() throws IOException {
        return intValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Integer.class);
            double doubleValue = Double.parseDouble(text());

            if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + text() + "] is out of range for an integer");
            }

            return (int) doubleValue;
        }
        int result = doIntValue();
        ensureNumberConversion(coerce, result, Integer.class);
        return result;
    }

    protected abstract int doIntValue() throws IOException;

    @Override
    public long longValue() throws IOException {
        return longValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Long.class);
            return Numbers.toLong(text(), coerce);
        }
        long result = doLongValue();
        ensureNumberConversion(coerce, result, Long.class);
        return result;
    }

    protected abstract long doLongValue() throws IOException;

    @Override
    public float floatValue() throws IOException {
        return floatValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Float.class);
            return Float.parseFloat(text());
        }
        return doFloatValue();
    }

    protected abstract float doFloatValue() throws IOException;

    @Override
    public double doubleValue() throws IOException {
        return doubleValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Double.class);
            return Double.parseDouble(text());
        }
        return doDoubleValue();
    }

    protected abstract double doDoubleValue() throws IOException;

    @Override
    public BigInteger bigIntegerValue() throws IOException {
        return bigIntegerValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public BigInteger bigIntegerValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, BigInteger.class);
            return Numbers.toUnsignedLong(text(), coerce);
        }
        BigInteger result = doBigIntegerValue();
        ensureNumberConversion(coerce, result, BigInteger.class);
        return result;
    }

    protected abstract BigInteger doBigIntegerValue() throws IOException;

    @Override
    public final String textOrNull() throws IOException {
        if (currentToken() == Token.VALUE_NULL) {
            return null;
        }
        return text();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        if (currentToken() == Token.VALUE_NULL) {
            return null;
        }
        return charBuffer();
    }

    @Override
    public Map<String, Object> map() throws IOException {
        return readMapSafe(this, SIMPLE_MAP_FACTORY);
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        return readMapSafe(this, ORDERED_MAP_FACTORY);
    }

    @Override
    public Map<String, String> mapStrings() throws IOException {
        return map(HashMap::new, XContentParser::text);
    }

    @Override
    public <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
        throws IOException {
        final Map<String, T> map = mapFactory.get();
        if (findNonEmptyMapStart(this) == false) {
            return map;
        }
        assert currentToken() == Token.FIELD_NAME : "Expected field name but saw [" + currentToken() + "]";
        do {
            // Must point to field name
            String fieldName = currentName();
            // And then the value...
            nextToken();
            T value = mapValueParser.apply(this);
            map.put(fieldName, value);
        } while (nextToken() == XContentParser.Token.FIELD_NAME);
        return map;
    }

    @Override
    public List<Object> list() throws IOException {
        skipToListStart(this);
        return readListUnsafe(this, SIMPLE_MAP_FACTORY);
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        skipToListStart(this);
        return readListUnsafe(this, ORDERED_MAP_FACTORY);
    }

    private static final Supplier<Map<String, Object>> SIMPLE_MAP_FACTORY = HashMap::new;

    private static final Supplier<Map<String, Object>> ORDERED_MAP_FACTORY = LinkedHashMap::new;

    private static Map<String, Object> readMapSafe(XContentParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        final Map<String, Object> map = mapFactory.get();
        return findNonEmptyMapStart(parser) ? readMapEntries(parser, mapFactory, map) : map;
    }

    // Read a map without bounds checks from a parser that is assumed to be at the map's first field's name token
    private static Map<String, Object> readMapEntries(
        XContentParser parser,
        Supplier<Map<String, Object>> mapFactory,
        Map<String, Object> map
    ) throws IOException {
        assert parser.currentToken() == Token.FIELD_NAME : "Expected field name but saw [" + parser.currentToken() + "]";
        do {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            Object value = readValueUnsafe(parser.nextToken(), parser, mapFactory);
            map.put(fieldName, value);
        } while (parser.nextToken() == Token.FIELD_NAME);
        return map;
    }

    /**
     * Checks if the next current token in the supplied parser is a map start for a non-empty map.
     * Skips to the next token if the parser does not yet have a current token (i.e. {@link #currentToken()} returns {@code null}) and then
     * checks it.
     *
     * @return true if a map start for a non-empty map is found
     */
    private static boolean findNonEmptyMapStart(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        return token == Token.FIELD_NAME;
    }

    // Skips the current parser to the next array start. Assumes that the parser is either positioned before an array field's name token or
    // on the start array token.
    private static void skipToListStart(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.START_ARRAY) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "Failed to parse list:  expecting " + XContentParser.Token.START_ARRAY + " but got " + token
            );
        }
    }

    // read a list without bounds checks, assuming the current parser is always on an array start
    private static List<Object> readListUnsafe(XContentParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        assert parser.currentToken() == Token.START_ARRAY;
        ArrayList<Object> list = new ArrayList<>();
        for (Token token = parser.nextToken(); token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
            list.add(readValueUnsafe(token, parser, mapFactory));
        }
        return list;
    }

    public static Object readValue(XContentParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        return readValueUnsafe(parser.currentToken(), parser, mapFactory);
    }

    /**
     * Reads next value from the parser that is assumed to be at the given current token without any additional checks.
     *
     * @param currentToken current token that the parser is at
     * @param parser       parser to read from
     * @param mapFactory   map factory to use for reading objects
     */
    private static Object readValueUnsafe(Token currentToken, XContentParser parser, Supplier<Map<String, Object>> mapFactory)
        throws IOException {
        assert currentToken == parser.currentToken() : "Supplied current token ["
            + currentToken
            + "] is different from actual parser current token ["
            + parser.currentToken()
            + "]";
        switch (currentToken) {
            case VALUE_STRING:
                return parser.text();
            case VALUE_NUMBER:
                return parser.numberValue();
            case VALUE_BOOLEAN:
                return parser.booleanValue();
            case START_OBJECT: {
                final Map<String, Object> map = mapFactory.get();
                return parser.nextToken() != Token.FIELD_NAME ? map : readMapEntries(parser, mapFactory, map);
            }
            case START_ARRAY:
                return readListUnsafe(parser, mapFactory);
            case VALUE_EMBEDDED_OBJECT:
                return parser.binaryValue();
            case VALUE_NULL:
            default:
                return null;
        }
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return xContentRegistry.parseNamedObject(categoryClass, name, this, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    @Override
    public abstract boolean isClosed();

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return deprecationHandler;
    }
}
