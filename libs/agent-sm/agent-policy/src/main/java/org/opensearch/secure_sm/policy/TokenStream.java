/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayDeque;
import java.util.Deque;

public class TokenStream {
    private final StreamTokenizer tokenizer;
    private final Deque<Token> buffer = new ArrayDeque<>();

    TokenStream(Reader reader) {
        this.tokenizer = Tokenizer.configureTokenizer(reader);
    }

    Token peek() throws IOException {
        if (buffer.isEmpty()) {
            buffer.push(nextToken());
        }
        return buffer.peek();
    }

    Token consume() throws IOException {
        return buffer.isEmpty() ? nextToken() : buffer.pop();
    }

    boolean isEOF() throws IOException {
        Token t = peek();
        return t.type() == StreamTokenizer.TT_EOF;
    }

    int line() throws IOException {
        return peek().line();
    }

    private Token nextToken() throws IOException {
        int type = tokenizer.nextToken();
        String text = switch (type) {
            case StreamTokenizer.TT_WORD, '"', '\'' -> tokenizer.sval;
            case StreamTokenizer.TT_EOF -> "<EOF>";
            default -> Character.toString((char) type);
        };
        return new Token(type, text, tokenizer.lineno());
    }
}
