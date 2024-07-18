/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.AbstractXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * JsonToStringParser is the main parser class to transform JSON into stringFields in a XContentParser
 * returns XContentParser with one parent field and subfields
 * fieldName, fieldName._value, fieldName._valueAndPath
 * @opensearch.internal
 */
public class JsonToStringXContentParser extends AbstractXContentParser {
    private final String fieldTypeName;
    private XContentParser parser;

    private ArrayList<String> valueList = new ArrayList<>();
    private ArrayList<String> valueAndPathList = new ArrayList<>();
    private ArrayList<String> keyList = new ArrayList<>();

    private XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);

    private NamedXContentRegistry xContentRegistry;

    private DeprecationHandler deprecationHandler;

    private String nullValue;

    private int ignoreAbove;

    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";
    private static final String DOT_SYMBOL = ".";
    private static final String EQUAL_SYMBOL = "=";

    public JsonToStringXContentParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        XContentParser parser,
        String fieldTypeName,
        String nullValue,
        int ignoreAbove
    ) throws IOException {
        super(xContentRegistry, deprecationHandler);
        this.deprecationHandler = deprecationHandler;
        this.xContentRegistry = xContentRegistry;
        this.parser = parser;
        this.fieldTypeName = fieldTypeName;
        this.nullValue = nullValue;
        this.ignoreAbove = ignoreAbove;
    }

    public XContentParser parseObject() throws IOException {
        builder.startObject();
        StringBuilder path = new StringBuilder(fieldTypeName);
        boolean isChildrenValueValid = parseToken(path, null, 1);
        removeKeyIfNeed(isChildrenValueValid);
        // deduplication the fieldName,valueList,valueAndPathList
        builder.field(this.fieldTypeName, new HashSet<>(keyList));
        builder.field(this.fieldTypeName + VALUE_SUFFIX, new HashSet<>(valueList));
        builder.field(this.fieldTypeName + VALUE_AND_PATH_SUFFIX, new HashSet<>(valueAndPathList));
        builder.endObject();
        String jString = XContentHelper.convertToJson(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON);
        return JsonXContent.jsonXContent.createParser(this.xContentRegistry, this.deprecationHandler, String.valueOf(jString));
    }

    /**
     * @return true if the child object contains no_null value, false otherwise
     */
    private boolean parseToken(StringBuilder path, String currentFieldName, int depth) throws IOException {

        if (depth == 1 && processNoNestedValue()) {
            return true;
        }
        boolean isChildrenValueValid = false;
        boolean lastBrotherValueValid = true;
        boolean visitFieldName = false;

        while (this.parser.nextToken() != Token.END_OBJECT) {
            if (this.parser.currentName() != null) {
                currentFieldName = this.parser.currentName();
            }

            if (this.parser.currentToken() == Token.FIELD_NAME) {
                visitFieldName = true;
                if (lastBrotherValueValid == false) {
                    removeKeyIfNeed(lastBrotherValueValid);
                }

                int dotIndex = currentFieldName.indexOf(DOT_SYMBOL);
                String fieldNameSuffix = currentFieldName;
                // The field name may be of the form foo.bar.baz
                // If that's the case, each "part" is a key.
                while (dotIndex >= 0) {
                    String fieldNamePrefix = fieldNameSuffix.substring(0, dotIndex);
                    if (!fieldNamePrefix.isEmpty()) {
                        this.keyList.add(fieldNamePrefix);
                    }
                    fieldNameSuffix = fieldNameSuffix.substring(dotIndex + 1);
                    dotIndex = fieldNameSuffix.indexOf(DOT_SYMBOL);
                }
                if (!fieldNameSuffix.isEmpty()) {
                    this.keyList.add(fieldNameSuffix);
                }
            } else if (this.parser.currentToken() == Token.START_ARRAY) {
                lastBrotherValueValid = parseToken(path, currentFieldName, depth);
                isChildrenValueValid |= lastBrotherValueValid;
                break;
            } else if (this.parser.currentToken() == Token.END_ARRAY) {
                // skip
            } else if (this.parser.currentToken() == Token.START_OBJECT) {
                path.append(DOT_SYMBOL).append(currentFieldName);
                boolean validValue = parseToken(path, currentFieldName, depth + 1);
                isChildrenValueValid |= validValue;
                removeKeyIfNeed(validValue);
                assert path.length() > (currentFieldName.length() - 1);
                path.setLength(path.length() - currentFieldName.length() - 1);
            } else {
                String parseValue = parseValue();
                if (parseValue != null) {
                    path.append(DOT_SYMBOL).append(currentFieldName);
                    this.valueList.add(parseValue);
                    this.valueAndPathList.add(path + EQUAL_SYMBOL + parseValue);
                    isChildrenValueValid = true;
                    lastBrotherValueValid = true;
                    assert path.length() > (currentFieldName.length() - 1);
                    path.setLength(path.length() - currentFieldName.length() - 1);
                } else {
                    lastBrotherValueValid = false;
                }
            }
        }
        if (visitFieldName && isChildrenValueValid == true) {
            removeKeyIfNeed(lastBrotherValueValid);
        }
        return isChildrenValueValid;
    }

    public void removeKeyIfNeed(boolean hasValidValue) {
        if (hasValidValue == false) {
            // it means that the value of the sub child (or the last brother) is invalid,
            // we should delete the key from keyList.
            assert keyList.size() > 0;
            this.keyList.remove(keyList.size() - 1);
        }
    }

    private boolean processNoNestedValue() throws IOException {
        if (parser.currentToken() == Token.VALUE_NULL) {
            if (nullValue != null) {
                this.valueList.add(nullValue);
            }
            return true;
        } else if (this.parser.currentToken() == Token.VALUE_STRING
            || this.parser.currentToken() == Token.VALUE_NUMBER
            || this.parser.currentToken() == Token.VALUE_BOOLEAN) {
                String value = this.parser.textOrNull();
                if (value != null && value.length() <= ignoreAbove) {
                    this.valueList.add(value);
                }
                return true;
            }
        return false;
    }

    private String parseValue() throws IOException {
        switch (this.parser.currentToken()) {
            case VALUE_BOOLEAN:
            case VALUE_NUMBER:
            case VALUE_STRING:
            case VALUE_NULL:
                return this.parser.textOrNull();
            // Handle other token types as needed
            case FIELD_NAME:
            case VALUE_EMBEDDED_OBJECT:
            case END_ARRAY:
            case START_ARRAY:
                break;
            default:
                throw new IOException("Unsupported token type [" + parser.currentToken() + "]");
        }
        return null;
    }

    @Override
    public MediaType contentType() {
        return MediaTypeRegistry.JSON;
    }

    @Override
    public Token nextToken() throws IOException {
        return this.parser.nextToken();
    }

    @Override
    public void skipChildren() throws IOException {
        this.parser.skipChildren();
    }

    @Override
    public Token currentToken() {
        return this.parser.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return this.parser.currentName();
    }

    @Override
    public String text() throws IOException {
        return this.parser.text();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return this.parser.charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return this.parser.objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return this.parser.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return this.parser.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return this.parser.textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return this.parser.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return this.parser.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return this.parser.numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return this.parser.numberType();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return this.parser.binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return this.parser.getTokenLocation();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return this.parser.booleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return this.parser.shortValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return this.parser.intValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return this.parser.longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        return this.parser.floatValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        return this.parser.doubleValue();
    }

    @Override
    protected BigInteger doBigIntegerValue() throws IOException {
        return this.parser.bigIntegerValue();
    }

    @Override
    public boolean isClosed() {
        return this.parser.isClosed();
    }

    @Override
    public void close() throws IOException {
        this.parser.close();
    }
}
