/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A storage-neutral description of how a field's column is laid out by a pluggable data format: an optional
 * value encoding followed by an optional block compression.
 *
 * <p>The mapping form is a single token or a list of tokens, with the encoding first:
 * <pre>
 *   "codec": "zstd(3)"
 *   "codec": ["delta", "zstd(3)"]
 *   "codec": ["dictionary", "lz4"]
 * </pre>
 *
 * <p>The vocabulary is open. Core defines the well-known tokens so they mean the same thing across formats:
 * encodings {@value #PLAIN}, {@value #DICTIONARY}, {@value #DELTA}, {@value #BYTE_SPLIT}, {@value #RLE} and
 * compressions {@value #ZSTD}, {@value #LZ4}, {@value #SNAPPY}, {@value #GZIP}, {@value #NONE} ({@value #ZSTD}
 * and {@value #GZIP} accept a level in parentheses). A data format may additionally accept tokens of its own; core
 * parses any well-formed token and leaves acceptance to the format's validator, which every format should express
 * through {@link #requireSupported(Collection)} so unsupported tokens are rejected at mapping time with a uniform
 * message. Tokens name intent, not an implementation: each format translates them to its own physical layout.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldCodec {

    /** Encoding: store values as-is. */
    public static final String PLAIN = "plain";
    /** Encoding: dictionary-encode repeated values. */
    public static final String DICTIONARY = "dictionary";
    /** Encoding: store deltas between consecutive values (integers, timestamps) or shared prefixes (strings). */
    public static final String DELTA = "delta";
    /** Encoding: split values into per-byte streams so the compressor sees homogeneous bytes (floats, integers). */
    public static final String BYTE_SPLIT = "byte_split";
    /** Encoding: run-length encode (booleans). */
    public static final String RLE = "rle";

    /** Compression: Zstandard, optional level 1-22. */
    public static final String ZSTD = "zstd";
    /** Compression: LZ4. */
    public static final String LZ4 = "lz4";
    /** Compression: Snappy. */
    public static final String SNAPPY = "snappy";
    /** Compression: gzip, optional level 1-9. */
    public static final String GZIP = "gzip";
    /** Compression: none. */
    public static final String NONE = "none";

    /** The well-known encoding tokens. */
    public static final Set<String> ENCODINGS = Set.of(PLAIN, DICTIONARY, DELTA, BYTE_SPLIT, RLE);
    /** The well-known compression tokens. */
    public static final Set<String> COMPRESSIONS = Set.of(ZSTD, LZ4, SNAPPY, GZIP, NONE);

    /** Well-known compressions that accept a level, with the inclusive [min, max] range. */
    private static final Map<String, int[]> LEVEL_RANGES = Map.of(ZSTD, new int[] { 1, 22 }, GZIP, new int[] { 1, 9 });

    private static final Pattern TOKEN = Pattern.compile("^([a-z][a-z_0-9]*)(?:\\((\\d{1,3})\\))?$");

    /**
     * One codec token: a name plus an optional level, e.g. {@code zstd(3)}.
     *
     * @param name  the lower-cased token name
     * @param level the level, or {@code null} when none was given
     */
    @ExperimentalApi
    public record Token(String name, @Nullable Integer level) {
        public Token {
            Objects.requireNonNull(name, "name");
        }

        /** Whether this is one of the well-known encoding tokens. */
        public boolean isKnownEncoding() {
            return ENCODINGS.contains(name);
        }

        /** Whether this is one of the well-known compression tokens. */
        public boolean isKnownCompression() {
            return COMPRESSIONS.contains(name);
        }

        /** Whether this token is defined by a data format rather than by core. */
        public boolean isFormatDefined() {
            return isKnownEncoding() == false && isKnownCompression() == false;
        }

        @Override
        public String toString() {
            return level == null ? name : name + "(" + level + ")";
        }
    }

    private final List<Token> tokens;

    private FieldCodec(List<Token> tokens) {
        this.tokens = List.copyOf(tokens);
    }

    /**
     * Parses the raw mapping value of a {@code codec} parameter: a string token or a list of one or two string tokens
     * (encoding first, then compression). Tokens are case-insensitive.
     *
     * <p>Core validates shape only: at most two tokens, well-known token classes not repeated, a well-known
     * compression never before a well-known encoding, and levels only where a well-known token accepts them (a
     * format-defined token may carry any level; its format validates it). Whether every token is actually
     * supported is decided by the data format's validator.
     *
     * @throws IllegalArgumentException if the value is malformed
     */
    public static FieldCodec parse(Object value) {
        List<String> raw = rawTokens(value);
        List<Token> tokens = new ArrayList<>(raw.size());
        boolean seenEncoding = false;
        boolean seenCompression = false;
        for (String text : raw) {
            Matcher m = TOKEN.matcher(text.trim().toLowerCase(Locale.ROOT));
            if (m.matches() == false) {
                throw new IllegalArgumentException(
                    "malformed codec token [" + text + "]; expected a name, optionally followed by a level such as zstd(3)"
                );
            }
            Token token = new Token(m.group(1), m.group(2) == null ? null : Integer.valueOf(m.group(2)));
            if (token.isKnownEncoding()) {
                if (seenEncoding) {
                    throw new IllegalArgumentException("codec [" + text + "] specifies a second encoding; only one is allowed");
                }
                if (seenCompression) {
                    throw new IllegalArgumentException("codec encoding [" + text + "] must precede the compression");
                }
                if (token.level() != null) {
                    throw new IllegalArgumentException("codec encoding [" + text + "] does not accept a level");
                }
                seenEncoding = true;
            } else if (token.isKnownCompression()) {
                if (seenCompression) {
                    throw new IllegalArgumentException("codec [" + text + "] specifies a second compression; only one is allowed");
                }
                if (token.level() != null) {
                    int[] range = LEVEL_RANGES.get(token.name());
                    if (range == null) {
                        throw new IllegalArgumentException("codec compression [" + token.name() + "] does not accept a level");
                    }
                    if (token.level() < range[0] || token.level() > range[1]) {
                        throw new IllegalArgumentException(
                            "codec compression ["
                                + token.name()
                                + "] level must be between "
                                + range[0]
                                + " and "
                                + range[1]
                                + ", got "
                                + token.level()
                        );
                    }
                }
                seenCompression = true;
            }
            tokens.add(token);
        }
        return new FieldCodec(tokens);
    }

    private static List<String> rawTokens(Object value) {
        if (value instanceof String s) {
            if (s.isBlank()) {
                throw new IllegalArgumentException("codec must not be empty");
            }
            return List.of(s);
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                throw new IllegalArgumentException("codec must not be empty");
            }
            if (list.size() > 2) {
                throw new IllegalArgumentException("codec accepts at most two tokens (an encoding and a compression), got " + list);
            }
            List<String> tokens = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof String s && s.isBlank() == false) {
                    tokens.add(s);
                } else {
                    throw new IllegalArgumentException("codec tokens must be non-empty strings, got " + list);
                }
            }
            return tokens;
        }
        throw new IllegalArgumentException("codec must be a string or a list of strings, got [" + value + "]");
    }

    /**
     * Rejects any token whose name is not in {@code supported}. Data formats call this from their validator so an
     * unsupported token, whether well-known or format-defined, fails at mapping time with a uniform message.
     *
     * @param supported the token names this format can honour
     * @throws IllegalArgumentException naming the first unsupported token and the supported set
     */
    public void requireSupported(Collection<String> supported) {
        for (Token token : tokens) {
            if (supported.contains(token.name()) == false) {
                throw new IllegalArgumentException(
                    "unsupported codec token [" + token.name() + "]; supported tokens: " + new TreeSet<>(supported)
                );
            }
        }
    }

    /** The parsed tokens in mapping order. */
    public List<Token> tokens() {
        return tokens;
    }

    /** The well-known encoding token name, or {@code null} when the codec names no well-known encoding. */
    @Nullable
    public String encoding() {
        for (Token token : tokens) {
            if (token.isKnownEncoding()) {
                return token.name();
            }
        }
        return null;
    }

    /** The well-known compression token name, or {@code null} when the codec names no well-known compression. */
    @Nullable
    public String compression() {
        for (Token token : tokens) {
            if (token.isKnownCompression()) {
                return token.name();
            }
        }
        return null;
    }

    /** The level of the well-known compression token, or {@code null} when unspecified or not applicable. */
    @Nullable
    public Integer compressionLevel() {
        for (Token token : tokens) {
            if (token.isKnownCompression()) {
                return token.level();
            }
        }
        return null;
    }

    /** The canonical token strings in mapping order, e.g. {@code ["delta", "zstd(3)"]}. */
    public List<String> tokenStrings() {
        List<String> strings = new ArrayList<>(tokens.size());
        for (Token token : tokens) {
            strings.add(token.toString());
        }
        return strings;
    }

    /** The value written back to the mapping: a single string when one token is set, otherwise the token list. */
    public Object toMappingValue() {
        List<String> strings = tokenStrings();
        return strings.size() == 1 ? strings.get(0) : strings;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof FieldCodec other && tokens.equals(other.tokens));
    }

    @Override
    public int hashCode() {
        return tokens.hashCode();
    }

    @Override
    public String toString() {
        return String.join(",", tokenStrings());
    }
}
