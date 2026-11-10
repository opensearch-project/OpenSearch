/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.util.HashSet;
import java.util.Set;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.python.antlr.Python3Lexer;
import org.opensearch.python.antlr.Python3Parser;
import org.opensearch.python.antlr.Python3ParserBaseListener;

public class PythonScriptUtility {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Check whether the passed-in python code is an expression
     * @param code python code
     * @return true if the code is an expression
     */
    public static boolean isCodeAnExpression(String code) {

        Python3Lexer lexer = new Python3Lexer(CharStreams.fromString(code));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Python3Parser parser = new Python3Parser(tokens);

        // Set error handler to fail fast on syntax errors
        parser.removeErrorListeners();
        parser.addErrorListener(new ThrowingErrorListener());

        try {
            // Parse the input as an expression (expr_stmt is the rule for expressions in Python)
            ParseTree tree = parser.expr();

            return parser.getNumberOfSyntaxErrors() == 0;
        } catch (ParseCancellationException e) {
            return false;
        }
    }

    // Custom error listener to handle parsing errors
    private static class ThrowingErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(
                Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int line,
                int charPositionInLine,
                String msg,
                RecognitionException e)
                throws ParseCancellationException {
            throw new ParseCancellationException("Invalid syntax: " + msg);
        }
    }

    /**
     * Parse the python code to extract accessed document column names
     * @param code python code
     * @return Set of accessed fields
     */
    public static Set<String> extractAccessedDocFields(String code) {
        Set<String> accessedDocFields = new HashSet<>();

        Python3Lexer lexer = new Python3Lexer(CharStreams.fromString(code));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Python3Parser parser = new Python3Parser(tokens);

        ParseTree tree = parser.file_input();

        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(new PythonDocFieldListener(accessedDocFields), tree);

        return accessedDocFields;
    }

    private static class PythonDocFieldListener extends Python3ParserBaseListener {
        private final Set<String> fields;

        public PythonDocFieldListener(Set<String> fields) {
            this.fields = fields;
        }

        @Override
        public void enterAtom_expr(Python3Parser.Atom_exprContext ctx) {
            if (ctx.atom() != null && ctx.trailer() != null && !ctx.trailer().isEmpty()) {
                // Check if this is a subscript expression: doc['ratings']
                if (ctx.atom().getText().equals("doc")) {
                    for (Python3Parser.TrailerContext trailerCtx : ctx.trailer()) {
                        if (trailerCtx.subscriptlist()
                                != null) { // Checks if it's subscript access: doc[...]
                            String key = trailerCtx.subscriptlist().getText();
                            // Extract key if it's a string literal
                            if (key.startsWith("'") || key.startsWith("\"")) {
                                fields.add(key.substring(1, key.length() - 1)); // Remove quotes
                            }
                        }
                    }
                }
            }
        }
    }
}
