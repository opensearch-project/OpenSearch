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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.xcontent;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.AbstractXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.ParseContext;
import java.io.IOException;
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
        parseToken(path);
        builder.field(this.fieldTypeName, keyList);
        builder.field(this.fieldTypeName + VALUE_SUFFIX, valueList);
        builder.field(this.fieldTypeName + VALUE_AND_PATH_SUFFIX, valueAndPathList);
        builder.endObject();
        String jString = XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
        return JsonXContent.jsonXContent.createParser(this.xContentRegistry, this.deprecationHandler, String.valueOf(jString));
    }

    private void parseToken(StringBuilder path) throws IOException {
        String currentFieldName;
        while (this.parser.nextToken() != Token.END_OBJECT) {
            currentFieldName = this.parser.currentName();
            StringBuilder parsedFields = new StringBuilder();
            if (this.parser.nextToken() == Token.START_OBJECT) {
                // TODO: consider to store the entire JsonObject at StartObject as string without changing the tokenizer position.
                path.append(DOT_SYMBOL + currentFieldName);
                this.keyList.add(currentFieldName);
                parseToken(path);
                int dotIndex = path.lastIndexOf(DOT_SYMBOL);
                if (dotIndex != -1) {
                    path.delete(dotIndex, path.length());
                }
            } else {
                path.append(DOT_SYMBOL + currentFieldName);
                parseValue(currentFieldName, parsedFields);
                this.keyList.add(currentFieldName);
                this.valueList.add(parsedFields.toString());
                this.valueAndPathList.add(path + EQUAL_SYMBOL + parsedFields);
                int dotIndex = path.lastIndexOf(DOT_SYMBOL);
                if (dotIndex != -1) {
                    path.delete(dotIndex, path.length());
                }
            }

        }
    }

    private void parseValue(String currentFieldName, StringBuilder parsedFields) throws IOException {
        switch (this.parser.currentToken()) {
            case VALUE_STRING:
                parsedFields.append(this.parser.textOrNull());
                break;
            // Handle other token types as needed
            case START_OBJECT:
                throw new IOException("Unsupported token type");
            case FIELD_NAME:
                break;
            case VALUE_EMBEDDED_OBJECT:
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
    public boolean isClosed() {
        return this.parser.isClosed();
    }

    @Override
    public void close() throws IOException {
        this.parser.close();
    }
}
