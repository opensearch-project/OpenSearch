/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
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
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * JsonToStringParser is the main parser class to transform JSON into stringFields in a XContentParser
 * returns XContentParser with one parent field and subfields
 * fieldName, fieldName._value, fieldName._valueAndPath
 * @opensearch.internal
 */
public class JsonToStringXContentParser extends AbstractXContentParser {
    private final String fieldTypeName;
    private final XContentParser parser;

    private final ArrayList<String> valueList = new ArrayList<>();
    private final ArrayList<String> valueAndPathList = new ArrayList<>();
    private final ArrayList<String> keyList = new ArrayList<>();

    private final XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);

    private final NamedXContentRegistry xContentRegistry;

    private final DeprecationHandler deprecationHandler;

    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";
    private static final String EQUAL_SYMBOL = "=";

    public JsonToStringXContentParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        XContentParser parser,
        String fieldTypeName
    ) throws IOException {
        super(xContentRegistry, deprecationHandler);
        this.deprecationHandler = deprecationHandler;
        this.xContentRegistry = xContentRegistry;
        this.parser = parser;
        this.fieldTypeName = fieldTypeName;
    }

    public XContentParser parseObject() throws IOException {
        assert currentToken() == Token.START_OBJECT;
        parser.nextToken(); // Skip the outer START_OBJECT. Need to return on END_OBJECT.

        builder.startObject();
        LinkedList<String> path = new LinkedList<>(Collections.singleton(fieldTypeName));
        while (currentToken() != Token.END_OBJECT) {
            parseToken(path);
        }
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
    private boolean parseToken(Deque<String> path) throws IOException {
        boolean isChildrenValueValid = false;
        boolean visitFieldName = false;
        if (this.parser.currentToken() == Token.FIELD_NAME) {
            final String currentFieldName = this.parser.currentName();
            path.addLast(currentFieldName); // Pushing onto the stack *must* be matched by pop
            visitFieldName = true;
            String parts = currentFieldName;
            while (parts.contains(".")) { // Extract the intermediate keys maybe present in fieldName
                int dotPos = parts.indexOf('.');
                String part = parts.substring(0, dotPos);
                this.keyList.add(part);
                parts = parts.substring(dotPos + 1);
            }
            this.keyList.add(parts); // parts has no dot, so either it's the original fieldName or it's the last part
            this.parser.nextToken(); // advance to the value of fieldName
            isChildrenValueValid = parseToken(path); // parse the value for fieldName (which will be an array, an object,
                                                     // or a primitive value)
            path.removeLast(); // Here is where we pop fieldName from the stack (since we're done with the value of fieldName)
            // Note that whichever other branch we just passed through has already ended with nextToken(), so we
            // don't need to call it.
        } else if (this.parser.currentToken() == Token.START_ARRAY) {
            parser.nextToken();
            while (this.parser.currentToken() != Token.END_ARRAY) {
                isChildrenValueValid |= parseToken(path);
            }
            this.parser.nextToken();
        } else if (this.parser.currentToken() == Token.START_OBJECT) {
            parser.nextToken();
            while (this.parser.currentToken() != Token.END_OBJECT) {
                isChildrenValueValid |= parseToken(path);
            }
            this.parser.nextToken();
        } else {
            String parsedValue = parseValue();
            if (parsedValue != null) {
                this.valueList.add(parsedValue);
                this.valueAndPathList.add(Strings.collectionToDelimitedString(path, ".") + EQUAL_SYMBOL + parsedValue);
                isChildrenValueValid = true;
            }
            this.parser.nextToken();
        }

        if (visitFieldName && isChildrenValueValid == false) {
            removeKeyOfNullValue();
        }
        return isChildrenValueValid;
    }

    public void removeKeyOfNullValue() {
        // it means that the value of the sub child (or the last brother) is invalid,
        // we should delete the key from keyList.
        assert keyList.size() > 0;
        this.keyList.remove(keyList.size() - 1);
    }

    private String parseValue() throws IOException {
        switch (this.parser.currentToken()) {
            case VALUE_BOOLEAN:
            case VALUE_NUMBER:
            case VALUE_STRING:
            case VALUE_NULL:
                return this.parser.textOrNull();
            // Handle other token types as needed
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected value token type [" + parser.currentToken() + "]");
        }
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
