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

package org.opensearch.cli;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Locale;

/**
 * A Terminal wraps access to reading input and writing output for a cli.
 *
 * The available methods are similar to those of {@link Console}, with the ability
 * to read either normal text or a password, and the ability to print a line
 * of text. Printing is also gated by the {@link Verbosity} of the terminal,
 * which allows {@link #println(Verbosity,String)} calls which act like a logger,
 * only actually printing if the verbosity level of the terminal is above
 * the verbosity of the message.
 * @see ConsoleTerminal
 * @see SystemTerminal
*/
public abstract class Terminal {

    /** Writer to standard error - not supplied by the {@link Console} API, so we share with subclasses */
    private static final PrintWriter ERROR_WRITER = newErrorWriter();

    /** The default terminal implementation, which will be a console if available, or stdout/stderr if not. */
    public static final Terminal DEFAULT = ConsoleTerminal.isSupported() ? new ConsoleTerminal() : new SystemTerminal();

    @SuppressForbidden(reason = "Writer for System.err")
    private static PrintWriter newErrorWriter() {
        return new PrintWriter(System.err);
    }

    /** Defines the available verbosity levels of messages to be printed.*/
    public enum Verbosity {
        /** always printed */
        SILENT,
        /** printed when no options are given to cli */
        NORMAL,
        /** printed only when cli is passed verbose option */
        VERBOSE
    }

    /** The current verbosity for the terminal, defaulting to {@link Verbosity#NORMAL}. */
    private Verbosity verbosity = Verbosity.NORMAL;

    /** The newline separator used when calling println. */
    private final String lineSeparator;

    /** Constructs a new terminal with the given line separator.
     * @param lineSeparator the line separator to use when calling println
     * */
    protected Terminal(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    /** Sets the {@link Terminal#verbosity} of the terminal. (Default is {@link Verbosity#NORMAL})
     * @param verbosity the {@link Verbosity} level that will be used for printing
     * */
    public void setVerbosity(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    /** Reads clear text from the terminal input.
     * @see Console#readLine()
     * @param prompt message to display to the user
     * @return the text entered by the user
     * */
    public abstract String readText(String prompt);

    /** Reads secret text from the terminal input with echoing disabled.
     * @see Console#readPassword()
     * @param prompt message to display to the user
     * @return the secret as a character array
     * */
    public abstract char[] readSecret(String prompt);

    /** Read secret text from terminal input with echoing disabled, up to a maximum length.
     * @see Console#readPassword()
     * @param prompt message to display to the user
     * @param maxLength the maximum length of the secret
     * @return the secret as a character array
     * @throws IllegalStateException if the secret exceeds the maximum length
     * */
    public char[] readSecret(String prompt, int maxLength) {
        char[] result = readSecret(prompt);
        if (result.length > maxLength) {
            Arrays.fill(result, '\0');
            throw new IllegalStateException("Secret exceeded maximum length of " + maxLength);
        }
        return result;
    }

    /** Returns a Writer which can be used to write to the terminal directly using standard output.
     * @return a writer to {@link Terminal#DEFAULT} output
     * @see Terminal.ConsoleTerminal
     * @see Terminal.SystemTerminal
     * */
    public abstract PrintWriter getWriter();

    /** Returns a Writer which can be used to write to the terminal directly using standard error.
     * @return a writer to stderr
     * */
    public PrintWriter getErrorWriter() {
        return ERROR_WRITER;
    }

    /** Prints a line to the terminal at {@link Verbosity#NORMAL} verbosity level, with a {@link Terminal#lineSeparator}
     * @param msg the message to print
     * */
    public final void println(String msg) {
        println(Verbosity.NORMAL, msg);
    }

    /** Prints message to the terminal's standard output at {@link Verbosity} level, with a {@link Terminal#lineSeparator}.
     * @param verbosity the {@link Verbosity} level at which to print
     * @param msg the message to print
     * */
    public final void println(Verbosity verbosity, String msg) {
        print(verbosity, msg + lineSeparator);
    }

    /** Prints message to the terminal's standard output at {@link Verbosity} level, without adding a {@link Terminal#lineSeparator}.
     * @param verbosity the {@link Verbosity} level at which to print
     * @param msg the message to print
     * */
    public final void print(Verbosity verbosity, String msg) {
        print(verbosity, msg, false);
    }

    /** Prints message to either standard or error output at {@link Verbosity} level, without adding a {@link Terminal#lineSeparator}.
     * @param verbosity the {@link Verbosity} level at which to print.
     * @param msg the message to print
     * @param isError if true, prints to standard error instead of standard output
     * */
    private void print(Verbosity verbosity, String msg, boolean isError) {
        if (isPrintable(verbosity)) {
            PrintWriter writer = isError ? getErrorWriter() : getWriter();
            writer.print(msg);
            writer.flush();
        }
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity} level, without adding a {@link Terminal#lineSeparator}.
     * @param verbosity the {@link Verbosity} level at which to print.
     * @param msg the message to print
     * */
    public final void errorPrint(Verbosity verbosity, String msg) {
        print(verbosity, msg, true);
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity#NORMAL} verbosity level, with a {@link Terminal#lineSeparator}
     * @param msg the message to print
     * */
    public final void errorPrintln(String msg) {
        errorPrintln(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity} level, with a {@link Terminal#lineSeparator}.
     * @param verbosity the {@link Verbosity} level at which to print.
     * @param msg the message to print
     * */
    public final void errorPrintln(Verbosity verbosity, String msg) {
        errorPrint(verbosity, msg + lineSeparator);
    }

    /** Checks if given {@link Verbosity} level is high enough to be printed at the level defined by {@link Terminal#verbosity}
     * @param verbosity the {@link Verbosity} level to check
     * @return true if the {@link Verbosity} level is high enough to be printed
     * @see Terminal#setVerbosity(Verbosity)
     * */
    public final boolean isPrintable(Verbosity verbosity) {
        return this.verbosity.ordinal() >= verbosity.ordinal();
    }

    /**
     * Prompt for a yes or no answer from the user. This method will loop until 'y', 'n'
     * (or the default empty value) is entered.
     * @param prompt the prompt to display to the user
     * @param defaultYes if true, the default answer is 'y', otherwise it is 'n'
     * @return true if the user answered 'y', false if the user answered 'n' or the defaultYes value if the user entered nothing
     */
    public final boolean promptYesNo(String prompt, boolean defaultYes) {
        String answerPrompt = defaultYes ? " [Y/n]" : " [y/N]";
        while (true) {
            String answer = readText(prompt + answerPrompt);
            if (answer == null || answer.isEmpty()) {
                return defaultYes;
            }
            answer = answer.toLowerCase(Locale.ROOT);
            boolean answerYes = answer.equals("y");
            if (answerYes == false && answer.equals("n") == false) {
                errorPrintln("Did not understand answer '" + answer + "'");
                continue;
            }
            return answerYes;
        }
    }

    /**
     * Read from the reader until we find a newline. If that newline
     * character is immediately preceded by a carriage return, we have
     * a Windows-style newline, so we discard the carriage return as well
     * as the newline.
     * @param reader the reader to read from
     * @param maxLength the maximum length of the line to read
     * @return the line read from the reader
     * @throws RuntimeException if the line read exceeds the maximum length
     * @throws RuntimeException if an IOException occurs while reading
     */
    public static char[] readLineToCharArray(Reader reader, int maxLength) {
        char[] buf = new char[maxLength + 2];
        try {
            int len = 0;
            int next;
            while ((next = reader.read()) != -1) {
                char nextChar = (char) next;
                if (nextChar == '\n') {
                    break;
                }
                if (len < buf.length) {
                    buf[len] = nextChar;
                }
                len++;
            }

            if (len > 0 && len < buf.length && buf[len - 1] == '\r') {
                len--;
            }

            if (len > maxLength) {
                Arrays.fill(buf, '\0');
                throw new RuntimeException("Input exceeded maximum length of " + maxLength);
            }

            char[] shortResult = Arrays.copyOf(buf, len);
            Arrays.fill(buf, '\0');
            return shortResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Flushes the terminal's standard output and standard error. */
    public void flush() {
        this.getWriter().flush();
        this.getErrorWriter().flush();
    }

    private static class ConsoleTerminal extends Terminal {

        private static final Console CONSOLE = System.console();

        ConsoleTerminal() {
            super(System.lineSeparator());
        }

        static boolean isSupported() {
            return CONSOLE != null;
        }

        @Override
        public PrintWriter getWriter() {
            return CONSOLE.writer();
        }

        @Override
        public String readText(String prompt) {
            return CONSOLE.readLine("%s", prompt);
        }

        @Override
        public char[] readSecret(String prompt) {
            return CONSOLE.readPassword("%s", prompt);
        }
    }

    /** visible for testing */
    static class SystemTerminal extends Terminal {

        private static final PrintWriter WRITER = newWriter();

        private BufferedReader reader;

        SystemTerminal() {
            super(System.lineSeparator());
        }

        @SuppressForbidden(reason = "Writer for System.out")
        private static PrintWriter newWriter() {
            return new PrintWriter(System.out);
        }

        /** visible for testing */
        BufferedReader getReader() {
            if (reader == null) {
                reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
            }
            return reader;
        }

        @Override
        public PrintWriter getWriter() {
            return WRITER;
        }

        @Override
        public String readText(String text) {
            getErrorWriter().print(text); // prompts should go to standard error to avoid mixing with list output
            try {
                final String line = getReader().readLine();
                if (line == null) {
                    throw new IllegalStateException("unable to read from standard input; is standard input open and a tty attached?");
                }
                return line;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public char[] readSecret(String text) {
            return readText(text).toCharArray();
        }

        @Override
        public char[] readSecret(String text, int maxLength) {
            getErrorWriter().println(text);
            return readLineToCharArray(getReader(), maxLength);
        }
    }
}
