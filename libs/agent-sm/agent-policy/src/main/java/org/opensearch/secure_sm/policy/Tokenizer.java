/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.io.Reader;
import java.io.StreamTokenizer;

public final class Tokenizer {

    private Tokenizer() {}

    /*
     * Configure the stream tokenizer:
     * Recognize strings between "..."
     * Don't convert words to lowercase
     * Recognize both C-style and C++-style comments
     * Treat end-of-line as white space, not as a token
     */

    // new Token(StreamTokenizer.TT_WORD, "grant", line) // keyword
    // new Token(StreamTokenizer.TT_WORD, "Codebase", line)
    // new Token('"', "file:/some/path", line) // quoted string
    // new Token('{', "{", line) // symbol
    // new Token(StreamTokenizer.TT_WORD, "permission", line)
    // new Token(StreamTokenizer.TT_WORD, "java.io.FilePermission", line)
    // new Token('"', "file", line)
    // new Token(',', ",", line)
    // new Token('"', "read", line)
    // new Token(';', ";", line)
    // new Token('}', "}", line)
    // new Token(';', ";", line)
    public static StreamTokenizer configureTokenizer(Reader reader) {
        StreamTokenizer st = new StreamTokenizer(reader);

        st.resetSyntax();
        st.wordChars('a', 'z');
        st.wordChars('A', 'Z');
        st.wordChars('.', '.');
        st.wordChars('0', '9');
        st.wordChars('_', '_');
        st.wordChars('$', '$');
        st.wordChars(128 + 32, 255); // extended chars
        st.whitespaceChars(0, ' ');
        st.commentChar('/');
        st.quoteChar('\'');
        st.quoteChar('"');
        st.lowerCaseMode(false);
        st.ordinaryChar('/');
        st.slashSlashComments(true);
        st.slashStarComments(true);

        return st;
    }
}
