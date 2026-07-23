/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 by Bart Kiers
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * Project      : python3-parser; an ANTLR4 grammar for Python 3
 *                https://github.com/bkiers/python3-parser
 * Developed by : Bart Kiers, bart@big-o.nl
 */

// $antlr-format alignTrailingComments true, columnLimit 150, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine true, allowShortBlocksOnASingleLine true, minEmptyLines 0, alignSemicolons ownLine
// $antlr-format alignColons trailing, singleLineOverrulesHangingColon true, alignLexerCommands true, alignLabels true, alignTrailers true

lexer grammar Python3Lexer;

// All comments that start with "///" are copy-pasted from
// The Python Language Reference

tokens {
    INDENT,
    DEDENT
}

options {
    superClass = Python3LexerBase;
}

// Insert here @header for C++ lexer.

/*
 * lexer rules
 */

STRING: STRING_LITERAL | BYTES_LITERAL;

NUMBER: INTEGER | FLOAT_NUMBER | IMAG_NUMBER;

INTEGER: DECIMAL_INTEGER | OCT_INTEGER | HEX_INTEGER | BIN_INTEGER;

AND        : 'and';
AS         : 'as';
ASSERT     : 'assert';
ASYNC      : 'async';
AWAIT      : 'await';
BREAK      : 'break';
CASE       : 'case';
CLASS      : 'class';
CONTINUE   : 'continue';
DEF        : 'def';
DEL        : 'del';
ELIF       : 'elif';
ELSE       : 'else';
EXCEPT     : 'except';
FALSE      : 'False';
FINALLY    : 'finally';
FOR        : 'for';
FROM       : 'from';
GLOBAL     : 'global';
IF         : 'if';
IMPORT     : 'import';
IN         : 'in';
IS         : 'is';
LAMBDA     : 'lambda';
MATCH      : 'match';
NONE       : 'None';
NONLOCAL   : 'nonlocal';
NOT        : 'not';
OR         : 'or';
PASS       : 'pass';
RAISE      : 'raise';
RETURN     : 'return';
TRUE       : 'True';
TRY        : 'try';
UNDERSCORE : '_';
WHILE      : 'while';
WITH       : 'with';
YIELD      : 'yield';

NEWLINE: ({this.atStartOfInput()}? SPACES | ( '\r'? '\n' | '\r' | '\f') SPACES?) {this.onNewLine();};

/// identifier   ::=  id_start id_continue*
NAME: ID_START ID_CONTINUE*;

/// stringliteral   ::=  [stringprefix](shortstring | longstring)
/// stringprefix    ::=  "r" | "u" | "R" | "U" | "f" | "F"
///                      | "fr" | "Fr" | "fR" | "FR" | "rf" | "rF" | "Rf" | "RF"
STRING_LITERAL: ( [rR] | [uU] | [fF] | ( [fF] [rR]) | ( [rR] [fF]))? ( SHORT_STRING | LONG_STRING);

/// bytesliteral   ::=  bytesprefix(shortbytes | longbytes)
/// bytesprefix    ::=  "b" | "B" | "br" | "Br" | "bR" | "BR" | "rb" | "rB" | "Rb" | "RB"
BYTES_LITERAL: ( [bB] | ( [bB] [rR]) | ( [rR] [bB])) ( SHORT_BYTES | LONG_BYTES);

/// decimalinteger ::=  nonzerodigit digit* | "0"+
DECIMAL_INTEGER: NON_ZERO_DIGIT DIGIT* | '0'+;

/// octinteger     ::=  "0" ("o" | "O") octdigit+
OCT_INTEGER: '0' [oO] OCT_DIGIT+;

/// hexinteger     ::=  "0" ("x" | "X") hexdigit+
HEX_INTEGER: '0' [xX] HEX_DIGIT+;

/// bininteger     ::=  "0" ("b" | "B") bindigit+
BIN_INTEGER: '0' [bB] BIN_DIGIT+;

/// floatnumber   ::=  pointfloat | exponentfloat
FLOAT_NUMBER: POINT_FLOAT | EXPONENT_FLOAT;

/// imagnumber ::=  (floatnumber | intpart) ("j" | "J")
IMAG_NUMBER: ( FLOAT_NUMBER | INT_PART) [jJ];

DOT                : '.';
ELLIPSIS           : '...';
STAR               : '*';
OPEN_PAREN         : '(' {this.openBrace();};
CLOSE_PAREN        : ')' {this.closeBrace();};
COMMA              : ',';
COLON              : ':';
SEMI_COLON         : ';';
POWER              : '**';
ASSIGN             : '=';
OPEN_BRACK         : '[' {this.openBrace();};
CLOSE_BRACK        : ']' {this.closeBrace();};
OR_OP              : '|';
XOR                : '^';
AND_OP             : '&';
LEFT_SHIFT         : '<<';
RIGHT_SHIFT        : '>>';
ADD                : '+';
MINUS              : '-';
DIV                : '/';
MOD                : '%';
IDIV               : '//';
NOT_OP             : '~';
OPEN_BRACE         : '{' {this.openBrace();};
CLOSE_BRACE        : '}' {this.closeBrace();};
LESS_THAN          : '<';
GREATER_THAN       : '>';
EQUALS             : '==';
GT_EQ              : '>=';
LT_EQ              : '<=';
NOT_EQ_1           : '<>';
NOT_EQ_2           : '!=';
AT                 : '@';
ARROW              : '->';
ADD_ASSIGN         : '+=';
SUB_ASSIGN         : '-=';
MULT_ASSIGN        : '*=';
AT_ASSIGN          : '@=';
DIV_ASSIGN         : '/=';
MOD_ASSIGN         : '%=';
AND_ASSIGN         : '&=';
OR_ASSIGN          : '|=';
XOR_ASSIGN         : '^=';
LEFT_SHIFT_ASSIGN  : '<<=';
RIGHT_SHIFT_ASSIGN : '>>=';
POWER_ASSIGN       : '**=';
IDIV_ASSIGN        : '//=';

SKIP_: ( SPACES | COMMENT | LINE_JOINING) -> skip;

UNKNOWN_CHAR: .;

/*
 * fragments
 */

/// shortstring     ::=  "'" shortstringitem* "'" | '"' shortstringitem* '"'
/// shortstringitem ::=  shortstringchar | stringescapeseq
/// shortstringchar ::=  <any source character except "\" or newline or the quote>
fragment SHORT_STRING:
    '\'' (STRING_ESCAPE_SEQ | ~[\\\r\n\f'])* '\''
    | '"' ( STRING_ESCAPE_SEQ | ~[\\\r\n\f"])* '"'
;
/// longstring      ::=  "'''" longstringitem* "'''" | '"""' longstringitem* '"""'
fragment LONG_STRING: '\'\'\'' LONG_STRING_ITEM*? '\'\'\'' | '"""' LONG_STRING_ITEM*? '"""';

/// longstringitem  ::=  longstringchar | stringescapeseq
fragment LONG_STRING_ITEM: LONG_STRING_CHAR | STRING_ESCAPE_SEQ;

/// longstringchar  ::=  <any source character except "\">
fragment LONG_STRING_CHAR: ~'\\';

/// stringescapeseq ::=  "\" <any source character>
fragment STRING_ESCAPE_SEQ: '\\' . | '\\' NEWLINE;

/// nonzerodigit   ::=  "1"..."9"
fragment NON_ZERO_DIGIT: [1-9];

/// digit          ::=  "0"..."9"
fragment DIGIT: [0-9];

/// octdigit       ::=  "0"..."7"
fragment OCT_DIGIT: [0-7];

/// hexdigit       ::=  digit | "a"..."f" | "A"..."F"
fragment HEX_DIGIT: [0-9a-fA-F];

/// bindigit       ::=  "0" | "1"
fragment BIN_DIGIT: [01];

/// pointfloat    ::=  [intpart] fraction | intpart "."
fragment POINT_FLOAT: INT_PART? FRACTION | INT_PART '.';

/// exponentfloat ::=  (intpart | pointfloat) exponent
fragment EXPONENT_FLOAT: ( INT_PART | POINT_FLOAT) EXPONENT;

/// intpart       ::=  digit+
fragment INT_PART: DIGIT+;

/// fraction      ::=  "." digit+
fragment FRACTION: '.' DIGIT+;

/// exponent      ::=  ("e" | "E") ["+" | "-"] digit+
fragment EXPONENT: [eE] [+-]? DIGIT+;

/// shortbytes     ::=  "'" shortbytesitem* "'" | '"' shortbytesitem* '"'
/// shortbytesitem ::=  shortbyteschar | bytesescapeseq
fragment SHORT_BYTES:
    '\'' (SHORT_BYTES_CHAR_NO_SINGLE_QUOTE | BYTES_ESCAPE_SEQ)* '\''
    | '"' ( SHORT_BYTES_CHAR_NO_DOUBLE_QUOTE | BYTES_ESCAPE_SEQ)* '"'
;

/// longbytes      ::=  "'''" longbytesitem* "'''" | '"""' longbytesitem* '"""'
fragment LONG_BYTES: '\'\'\'' LONG_BYTES_ITEM*? '\'\'\'' | '"""' LONG_BYTES_ITEM*? '"""';

/// longbytesitem  ::=  longbyteschar | bytesescapeseq
fragment LONG_BYTES_ITEM: LONG_BYTES_CHAR | BYTES_ESCAPE_SEQ;

/// shortbyteschar ::=  <any ASCII character except "\" or newline or the quote>
fragment SHORT_BYTES_CHAR_NO_SINGLE_QUOTE:
    [\u0000-\u0009]
    | [\u000B-\u000C]
    | [\u000E-\u0026]
    | [\u0028-\u005B]
    | [\u005D-\u007F]
;

fragment SHORT_BYTES_CHAR_NO_DOUBLE_QUOTE:
    [\u0000-\u0009]
    | [\u000B-\u000C]
    | [\u000E-\u0021]
    | [\u0023-\u005B]
    | [\u005D-\u007F]
;

/// longbyteschar  ::=  <any ASCII character except "\">
fragment LONG_BYTES_CHAR: [\u0000-\u005B] | [\u005D-\u007F];

/// bytesescapeseq ::=  "\" <any ASCII character>
fragment BYTES_ESCAPE_SEQ: '\\' [\u0000-\u007F];

fragment SPACES: [ \t]+;

fragment COMMENT: '#' ~[\r\n\f]*;

fragment LINE_JOINING: '\\' SPACES? ( '\r'? '\n' | '\r' | '\f');

// TODO: ANTLR seems lack of some Unicode property support...
//$ curl https://www.unicode.org/Public/13.0.0/ucd/PropList.txt | grep Other_ID_
//1885..1886    ; Other_ID_Start # Mn   [2] MONGOLIAN LETTER ALI GALI BALUDA..MONGOLIAN LETTER ALI GALI THREE BALUDA
//2118          ; Other_ID_Start # Sm       SCRIPT CAPITAL P
//212E          ; Other_ID_Start # So       ESTIMATED SYMBOL
//309B..309C    ; Other_ID_Start # Sk   [2] KATAKANA-HIRAGANA VOICED SOUND MARK..KATAKANA-HIRAGANA SEMI-VOICED SOUND MARK
//00B7          ; Other_ID_Continue # Po       MIDDLE DOT
//0387          ; Other_ID_Continue # Po       GREEK ANO TELEIA
//1369..1371    ; Other_ID_Continue # No   [9] ETHIOPIC DIGIT ONE..ETHIOPIC DIGIT NINE
//19DA          ; Other_ID_Continue # No       NEW TAI LUE THAM DIGIT ONE

fragment UNICODE_OIDS: '\u1885' ..'\u1886' | '\u2118' | '\u212e' | '\u309b' ..'\u309c';

fragment UNICODE_OIDC: '\u00b7' | '\u0387' | '\u1369' ..'\u1371' | '\u19da';

/// id_start     ::=  <all characters in general categories Lu, Ll, Lt, Lm, Lo, Nl, the underscore, and characters with the Other_ID_Start property>
fragment ID_START:
    '_'
    | [\p{L}]
    | [\p{Nl}]
    //| [\p{Other_ID_Start}]
    | UNICODE_OIDS
;

/// id_continue  ::=  <all characters in id_start, plus characters in the categories Mn, Mc, Nd, Pc and others with the Other_ID_Continue property>
fragment ID_CONTINUE:
    ID_START
    | [\p{Mn}]
    | [\p{Mc}]
    | [\p{Nd}]
    | [\p{Pc}]
    //| [\p{Other_ID_Continue}]
    | UNICODE_OIDC
;