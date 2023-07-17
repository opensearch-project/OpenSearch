/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.AbstractXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.ParseContext;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.ArrayList;

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
    private ParseContext parseContext;

    private NamedXContentRegistry xContentRegistry;

    private DeprecationHandler deprecationHandler;

    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";
    private static final String DOT_SYMBOL = ".";
    private static final String EQUAL_SYMBOL = "=";

    public JsonToStringXContentParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        ParseContext parseContext,
        String fieldTypeName
    ) throws IOException {
        super(xContentRegistry, deprecationHandler);
        this.parseContext = parseContext;
        this.deprecationHandler = deprecationHandler;
        this.xContentRegistry = xContentRegistry;
        this.parser = parseContext.parser();
        this.fieldTypeName = fieldTypeName;
    }

    public XContentParser parseObject() throws IOException {
        builder.startObject();
        StringBuilder path = new StringBuilder(fieldTypeName);
        parseToken(path, null);
        builder.field(this.fieldTypeName, keyList);
        builder.field(this.fieldTypeName + VALUE_SUFFIX, valueList);
        builder.field(this.fieldTypeName + VALUE_AND_PATH_SUFFIX, valueAndPathList);
        builder.endObject();
        String jString = XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
        return JsonXContent.jsonXContent.createParser(this.xContentRegistry, this.deprecationHandler, String.valueOf(jString));
    }

    private void parseToken(StringBuilder path, String currentFieldName) throws IOException {

        while (this.parser.nextToken() != Token.END_OBJECT) {
            if (this.parser.currentName() != null) {
                currentFieldName = this.parser.currentName();
            }
            StringBuilder parsedFields = new StringBuilder();

            if (this.parser.currentToken() == Token.FIELD_NAME) {
                path.append(DOT_SYMBOL + currentFieldName);
                this.keyList.add(currentFieldName);
            } else if (this.parser.currentToken() == Token.START_ARRAY) {
                parseToken(path, currentFieldName);
                break;
            } else if (this.parser.currentToken() == Token.END_ARRAY) {
                // skip
            } else if (this.parser.currentToken() == Token.START_OBJECT) {
                parseToken(path, currentFieldName);
                int dotIndex = path.lastIndexOf(DOT_SYMBOL);
                if (dotIndex != -1) {
                    path.delete(dotIndex, path.length());
                }
            } else {
                if (!path.toString().contains(currentFieldName)) {
                    path.append(DOT_SYMBOL + currentFieldName);
                }
                parseValue(parsedFields);
                this.valueList.add(parsedFields.toString());
                this.valueAndPathList.add(path + EQUAL_SYMBOL + parsedFields);
                int dotIndex = path.lastIndexOf(DOT_SYMBOL);
                if (dotIndex != -1) {
                    path.delete(dotIndex, path.length());
                }
            }

        }
    }

    private void parseValue(StringBuilder parsedFields) throws IOException {
        switch (this.parser.currentToken()) {
            case VALUE_BOOLEAN:
            case VALUE_NUMBER:
            case VALUE_STRING:
            case VALUE_NULL:
                parsedFields.append(this.parser.textOrNull());
                break;
            // Handle other token types as needed
            case FIELD_NAME:
            case VALUE_EMBEDDED_OBJECT:
            case END_ARRAY:
            case START_ARRAY:
                break;
            default:
                throw new IOException("Unsupported token type [" + parser.currentToken() + "]");
        }
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
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
