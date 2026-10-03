/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

// Generated from antlr/Python3Parser.g4 by ANTLR 4.13.2

package org.opensearch.python.antlr;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class Python3Parser extends Python3ParserBase {
	static { RuntimeMetaData.checkVersion("4.13.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		INDENT=1, DEDENT=2, STRING=3, NUMBER=4, INTEGER=5, AND=6, AS=7, ASSERT=8,
		ASYNC=9, AWAIT=10, BREAK=11, CASE=12, CLASS=13, CONTINUE=14, DEF=15, DEL=16,
		ELIF=17, ELSE=18, EXCEPT=19, FALSE=20, FINALLY=21, FOR=22, FROM=23, GLOBAL=24,
		IF=25, IMPORT=26, IN=27, IS=28, LAMBDA=29, MATCH=30, NONE=31, NONLOCAL=32,
		NOT=33, OR=34, PASS=35, RAISE=36, RETURN=37, TRUE=38, TRY=39, UNDERSCORE=40,
		WHILE=41, WITH=42, YIELD=43, NEWLINE=44, NAME=45, STRING_LITERAL=46, BYTES_LITERAL=47,
		DECIMAL_INTEGER=48, OCT_INTEGER=49, HEX_INTEGER=50, BIN_INTEGER=51, FLOAT_NUMBER=52,
		IMAG_NUMBER=53, DOT=54, ELLIPSIS=55, STAR=56, OPEN_PAREN=57, CLOSE_PAREN=58,
		COMMA=59, COLON=60, SEMI_COLON=61, POWER=62, ASSIGN=63, OPEN_BRACK=64,
		CLOSE_BRACK=65, OR_OP=66, XOR=67, AND_OP=68, LEFT_SHIFT=69, RIGHT_SHIFT=70,
		ADD=71, MINUS=72, DIV=73, MOD=74, IDIV=75, NOT_OP=76, OPEN_BRACE=77, CLOSE_BRACE=78,
		LESS_THAN=79, GREATER_THAN=80, EQUALS=81, GT_EQ=82, LT_EQ=83, NOT_EQ_1=84,
		NOT_EQ_2=85, AT=86, ARROW=87, ADD_ASSIGN=88, SUB_ASSIGN=89, MULT_ASSIGN=90,
		AT_ASSIGN=91, DIV_ASSIGN=92, MOD_ASSIGN=93, AND_ASSIGN=94, OR_ASSIGN=95,
		XOR_ASSIGN=96, LEFT_SHIFT_ASSIGN=97, RIGHT_SHIFT_ASSIGN=98, POWER_ASSIGN=99,
		IDIV_ASSIGN=100, SKIP_=101, UNKNOWN_CHAR=102;
	public static final int
		RULE_single_input = 0, RULE_file_input = 1, RULE_eval_input = 2, RULE_decorator = 3,
		RULE_decorators = 4, RULE_decorated = 5, RULE_async_funcdef = 6, RULE_funcdef = 7,
		RULE_parameters = 8, RULE_typedargslist = 9, RULE_tfpdef = 10, RULE_varargslist = 11,
		RULE_vfpdef = 12, RULE_stmt = 13, RULE_simple_stmts = 14, RULE_simple_stmt = 15,
		RULE_expr_stmt = 16, RULE_annassign = 17, RULE_testlist_star_expr = 18,
		RULE_augassign = 19, RULE_del_stmt = 20, RULE_pass_stmt = 21, RULE_flow_stmt = 22,
		RULE_break_stmt = 23, RULE_continue_stmt = 24, RULE_return_stmt = 25,
		RULE_yield_stmt = 26, RULE_raise_stmt = 27, RULE_import_stmt = 28, RULE_import_name = 29,
		RULE_import_from = 30, RULE_import_as_name = 31, RULE_dotted_as_name = 32,
		RULE_import_as_names = 33, RULE_dotted_as_names = 34, RULE_dotted_name = 35,
		RULE_global_stmt = 36, RULE_nonlocal_stmt = 37, RULE_assert_stmt = 38,
		RULE_compound_stmt = 39, RULE_async_stmt = 40, RULE_if_stmt = 41, RULE_while_stmt = 42,
		RULE_for_stmt = 43, RULE_try_stmt = 44, RULE_with_stmt = 45, RULE_with_item = 46,
		RULE_except_clause = 47, RULE_block = 48, RULE_match_stmt = 49, RULE_subject_expr = 50,
		RULE_star_named_expressions = 51, RULE_star_named_expression = 52, RULE_case_block = 53,
		RULE_guard = 54, RULE_patterns = 55, RULE_pattern = 56, RULE_as_pattern = 57,
		RULE_or_pattern = 58, RULE_closed_pattern = 59, RULE_literal_pattern = 60,
		RULE_literal_expr = 61, RULE_complex_number = 62, RULE_signed_number = 63,
		RULE_signed_real_number = 64, RULE_real_number = 65, RULE_imaginary_number = 66,
		RULE_capture_pattern = 67, RULE_pattern_capture_target = 68, RULE_wildcard_pattern = 69,
		RULE_value_pattern = 70, RULE_attr = 71, RULE_name_or_attr = 72, RULE_group_pattern = 73,
		RULE_sequence_pattern = 74, RULE_open_sequence_pattern = 75, RULE_maybe_sequence_pattern = 76,
		RULE_maybe_star_pattern = 77, RULE_star_pattern = 78, RULE_mapping_pattern = 79,
		RULE_items_pattern = 80, RULE_key_value_pattern = 81, RULE_double_star_pattern = 82,
		RULE_class_pattern = 83, RULE_positional_patterns = 84, RULE_keyword_patterns = 85,
		RULE_keyword_pattern = 86, RULE_test = 87, RULE_test_nocond = 88, RULE_lambdef = 89,
		RULE_lambdef_nocond = 90, RULE_or_test = 91, RULE_and_test = 92, RULE_not_test = 93,
		RULE_comparison = 94, RULE_comp_op = 95, RULE_star_expr = 96, RULE_expr = 97,
		RULE_atom_expr = 98, RULE_atom = 99, RULE_name = 100, RULE_testlist_comp = 101,
		RULE_trailer = 102, RULE_subscriptlist = 103, RULE_subscript_ = 104, RULE_sliceop = 105,
		RULE_exprlist = 106, RULE_testlist = 107, RULE_dictorsetmaker = 108, RULE_classdef = 109,
		RULE_arglist = 110, RULE_argument = 111, RULE_comp_iter = 112, RULE_comp_for = 113,
		RULE_comp_if = 114, RULE_encoding_decl = 115, RULE_yield_expr = 116, RULE_yield_arg = 117,
		RULE_strings = 118;
	private static String[] makeRuleNames() {
		return new String[] {
			"single_input", "file_input", "eval_input", "decorator", "decorators",
			"decorated", "async_funcdef", "funcdef", "parameters", "typedargslist",
			"tfpdef", "varargslist", "vfpdef", "stmt", "simple_stmts", "simple_stmt",
			"expr_stmt", "annassign", "testlist_star_expr", "augassign", "del_stmt",
			"pass_stmt", "flow_stmt", "break_stmt", "continue_stmt", "return_stmt",
			"yield_stmt", "raise_stmt", "import_stmt", "import_name", "import_from",
			"import_as_name", "dotted_as_name", "import_as_names", "dotted_as_names",
			"dotted_name", "global_stmt", "nonlocal_stmt", "assert_stmt", "compound_stmt",
			"async_stmt", "if_stmt", "while_stmt", "for_stmt", "try_stmt", "with_stmt",
			"with_item", "except_clause", "block", "match_stmt", "subject_expr",
			"star_named_expressions", "star_named_expression", "case_block", "guard",
			"patterns", "pattern", "as_pattern", "or_pattern", "closed_pattern",
			"literal_pattern", "literal_expr", "complex_number", "signed_number",
			"signed_real_number", "real_number", "imaginary_number", "capture_pattern",
			"pattern_capture_target", "wildcard_pattern", "value_pattern", "attr",
			"name_or_attr", "group_pattern", "sequence_pattern", "open_sequence_pattern",
			"maybe_sequence_pattern", "maybe_star_pattern", "star_pattern", "mapping_pattern",
			"items_pattern", "key_value_pattern", "double_star_pattern", "class_pattern",
			"positional_patterns", "keyword_patterns", "keyword_pattern", "test",
			"test_nocond", "lambdef", "lambdef_nocond", "or_test", "and_test", "not_test",
			"comparison", "comp_op", "star_expr", "expr", "atom_expr", "atom", "name",
			"testlist_comp", "trailer", "subscriptlist", "subscript_", "sliceop",
			"exprlist", "testlist", "dictorsetmaker", "classdef", "arglist", "argument",
			"comp_iter", "comp_for", "comp_if", "encoding_decl", "yield_expr", "yield_arg",
			"strings"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, null, null, null, null, null, "'and'", "'as'", "'assert'", "'async'",
			"'await'", "'break'", "'case'", "'class'", "'continue'", "'def'", "'del'",
			"'elif'", "'else'", "'except'", "'False'", "'finally'", "'for'", "'from'",
			"'global'", "'if'", "'import'", "'in'", "'is'", "'lambda'", "'match'",
			"'None'", "'nonlocal'", "'not'", "'or'", "'pass'", "'raise'", "'return'",
			"'True'", "'try'", "'_'", "'while'", "'with'", "'yield'", null, null,
			null, null, null, null, null, null, null, null, "'.'", "'...'", "'*'",
			"'('", "')'", "','", "':'", "';'", "'**'", "'='", "'['", "']'", "'|'",
			"'^'", "'&'", "'<<'", "'>>'", "'+'", "'-'", "'/'", "'%'", "'//'", "'~'",
			"'{'", "'}'", "'<'", "'>'", "'=='", "'>='", "'<='", "'<>'", "'!='", "'@'",
			"'->'", "'+='", "'-='", "'*='", "'@='", "'/='", "'%='", "'&='", "'|='",
			"'^='", "'<<='", "'>>='", "'**='", "'//='"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "INDENT", "DEDENT", "STRING", "NUMBER", "INTEGER", "AND", "AS",
			"ASSERT", "ASYNC", "AWAIT", "BREAK", "CASE", "CLASS", "CONTINUE", "DEF",
			"DEL", "ELIF", "ELSE", "EXCEPT", "FALSE", "FINALLY", "FOR", "FROM", "GLOBAL",
			"IF", "IMPORT", "IN", "IS", "LAMBDA", "MATCH", "NONE", "NONLOCAL", "NOT",
			"OR", "PASS", "RAISE", "RETURN", "TRUE", "TRY", "UNDERSCORE", "WHILE",
			"WITH", "YIELD", "NEWLINE", "NAME", "STRING_LITERAL", "BYTES_LITERAL",
			"DECIMAL_INTEGER", "OCT_INTEGER", "HEX_INTEGER", "BIN_INTEGER", "FLOAT_NUMBER",
			"IMAG_NUMBER", "DOT", "ELLIPSIS", "STAR", "OPEN_PAREN", "CLOSE_PAREN",
			"COMMA", "COLON", "SEMI_COLON", "POWER", "ASSIGN", "OPEN_BRACK", "CLOSE_BRACK",
			"OR_OP", "XOR", "AND_OP", "LEFT_SHIFT", "RIGHT_SHIFT", "ADD", "MINUS",
			"DIV", "MOD", "IDIV", "NOT_OP", "OPEN_BRACE", "CLOSE_BRACE", "LESS_THAN",
			"GREATER_THAN", "EQUALS", "GT_EQ", "LT_EQ", "NOT_EQ_1", "NOT_EQ_2", "AT",
			"ARROW", "ADD_ASSIGN", "SUB_ASSIGN", "MULT_ASSIGN", "AT_ASSIGN", "DIV_ASSIGN",
			"MOD_ASSIGN", "AND_ASSIGN", "OR_ASSIGN", "XOR_ASSIGN", "LEFT_SHIFT_ASSIGN",
			"RIGHT_SHIFT_ASSIGN", "POWER_ASSIGN", "IDIV_ASSIGN", "SKIP_", "UNKNOWN_CHAR"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Python3Parser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public Python3Parser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Single_inputContext extends ParserRuleContext {
		public TerminalNode NEWLINE() { return getToken(Python3Parser.NEWLINE, 0); }
		public Simple_stmtsContext simple_stmts() {
			return getRuleContext(Simple_stmtsContext.class,0);
		}
		public Compound_stmtContext compound_stmt() {
			return getRuleContext(Compound_stmtContext.class,0);
		}
		public Single_inputContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_single_input; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSingle_input(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSingle_input(this);
		}
	}

	public final Single_inputContext single_input() throws RecognitionException {
		Single_inputContext _localctx = new Single_inputContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_single_input);
		try {
			setState(243);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(238);
				match(NEWLINE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(239);
				simple_stmts();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(240);
				compound_stmt();
				setState(241);
				match(NEWLINE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class File_inputContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(Python3Parser.EOF, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(Python3Parser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(Python3Parser.NEWLINE, i);
		}
		public List<StmtContext> stmt() {
			return getRuleContexts(StmtContext.class);
		}
		public StmtContext stmt(int i) {
			return getRuleContext(StmtContext.class,i);
		}
		public File_inputContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_file_input; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterFile_input(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitFile_input(this);
		}
	}

	public final File_inputContext file_input() throws RecognitionException {
		File_inputContext _localctx = new File_inputContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_file_input);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & 252271930291384088L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 4206977L) != 0)) {
				{
				setState(247);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case NEWLINE:
					{
					setState(245);
					match(NEWLINE);
					}
					break;
				case STRING:
				case NUMBER:
				case ASSERT:
				case ASYNC:
				case AWAIT:
				case BREAK:
				case CLASS:
				case CONTINUE:
				case DEF:
				case DEL:
				case FALSE:
				case FOR:
				case FROM:
				case GLOBAL:
				case IF:
				case IMPORT:
				case LAMBDA:
				case MATCH:
				case NONE:
				case NONLOCAL:
				case NOT:
				case PASS:
				case RAISE:
				case RETURN:
				case TRUE:
				case TRY:
				case UNDERSCORE:
				case WHILE:
				case WITH:
				case YIELD:
				case NAME:
				case ELLIPSIS:
				case STAR:
				case OPEN_PAREN:
				case OPEN_BRACK:
				case ADD:
				case MINUS:
				case NOT_OP:
				case OPEN_BRACE:
				case AT:
					{
					setState(246);
					stmt();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(251);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(252);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Eval_inputContext extends ParserRuleContext {
		public TestlistContext testlist() {
			return getRuleContext(TestlistContext.class,0);
		}
		public TerminalNode EOF() { return getToken(Python3Parser.EOF, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(Python3Parser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(Python3Parser.NEWLINE, i);
		}
		public Eval_inputContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_eval_input; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterEval_input(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitEval_input(this);
		}
	}

	public final Eval_inputContext eval_input() throws RecognitionException {
		Eval_inputContext _localctx = new Eval_inputContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_eval_input);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(254);
			testlist();
			setState(258);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==NEWLINE) {
				{
				{
				setState(255);
				match(NEWLINE);
				}
				}
				setState(260);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(261);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DecoratorContext extends ParserRuleContext {
		public TerminalNode AT() { return getToken(Python3Parser.AT, 0); }
		public Dotted_nameContext dotted_name() {
			return getRuleContext(Dotted_nameContext.class,0);
		}
		public TerminalNode NEWLINE() { return getToken(Python3Parser.NEWLINE, 0); }
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public ArglistContext arglist() {
			return getRuleContext(ArglistContext.class,0);
		}
		public DecoratorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_decorator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDecorator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDecorator(this);
		}
	}

	public final DecoratorContext decorator() throws RecognitionException {
		DecoratorContext _localctx = new DecoratorContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_decorator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(263);
			match(AT);
			setState(264);
			dotted_name();
			setState(270);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPEN_PAREN) {
				{
				setState(265);
				match(OPEN_PAREN);
				setState(267);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4863924168670839832L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(266);
					arglist();
					}
				}

				setState(269);
				match(CLOSE_PAREN);
				}
			}

			setState(272);
			match(NEWLINE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DecoratorsContext extends ParserRuleContext {
		public List<DecoratorContext> decorator() {
			return getRuleContexts(DecoratorContext.class);
		}
		public DecoratorContext decorator(int i) {
			return getRuleContext(DecoratorContext.class,i);
		}
		public DecoratorsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_decorators; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDecorators(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDecorators(this);
		}
	}

	public final DecoratorsContext decorators() throws RecognitionException {
		DecoratorsContext _localctx = new DecoratorsContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_decorators);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(275);
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(274);
				decorator();
				}
				}
				setState(277);
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==AT );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DecoratedContext extends ParserRuleContext {
		public DecoratorsContext decorators() {
			return getRuleContext(DecoratorsContext.class,0);
		}
		public ClassdefContext classdef() {
			return getRuleContext(ClassdefContext.class,0);
		}
		public FuncdefContext funcdef() {
			return getRuleContext(FuncdefContext.class,0);
		}
		public Async_funcdefContext async_funcdef() {
			return getRuleContext(Async_funcdefContext.class,0);
		}
		public DecoratedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_decorated; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDecorated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDecorated(this);
		}
	}

	public final DecoratedContext decorated() throws RecognitionException {
		DecoratedContext _localctx = new DecoratedContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_decorated);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(279);
			decorators();
			setState(283);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CLASS:
				{
				setState(280);
				classdef();
				}
				break;
			case DEF:
				{
				setState(281);
				funcdef();
				}
				break;
			case ASYNC:
				{
				setState(282);
				async_funcdef();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Async_funcdefContext extends ParserRuleContext {
		public TerminalNode ASYNC() { return getToken(Python3Parser.ASYNC, 0); }
		public FuncdefContext funcdef() {
			return getRuleContext(FuncdefContext.class,0);
		}
		public Async_funcdefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_async_funcdef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAsync_funcdef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAsync_funcdef(this);
		}
	}

	public final Async_funcdefContext async_funcdef() throws RecognitionException {
		Async_funcdefContext _localctx = new Async_funcdefContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_async_funcdef);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(285);
			match(ASYNC);
			setState(286);
			funcdef();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FuncdefContext extends ParserRuleContext {
		public TerminalNode DEF() { return getToken(Python3Parser.DEF, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public ParametersContext parameters() {
			return getRuleContext(ParametersContext.class,0);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public BlockContext block() {
			return getRuleContext(BlockContext.class,0);
		}
		public TerminalNode ARROW() { return getToken(Python3Parser.ARROW, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public FuncdefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_funcdef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterFuncdef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitFuncdef(this);
		}
	}

	public final FuncdefContext funcdef() throws RecognitionException {
		FuncdefContext _localctx = new FuncdefContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_funcdef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(288);
			match(DEF);
			setState(289);
			name();
			setState(290);
			parameters();
			setState(293);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ARROW) {
				{
				setState(291);
				match(ARROW);
				setState(292);
				test();
				}
			}

			setState(295);
			match(COLON);
			setState(296);
			block();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ParametersContext extends ParserRuleContext {
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public TypedargslistContext typedargslist() {
			return getRuleContext(TypedargslistContext.class,0);
		}
		public ParametersContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameters; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterParameters(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitParameters(this);
		}
	}

	public final ParametersContext parameters() throws RecognitionException {
		ParametersContext _localctx = new ParametersContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_parameters);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(298);
			match(OPEN_PAREN);
			setState(300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4683779897422774272L) != 0)) {
				{
				setState(299);
				typedargslist();
				}
			}

			setState(302);
			match(CLOSE_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TypedargslistContext extends ParserRuleContext {
		public List<TfpdefContext> tfpdef() {
			return getRuleContexts(TfpdefContext.class);
		}
		public TfpdefContext tfpdef(int i) {
			return getRuleContext(TfpdefContext.class,i);
		}
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public TerminalNode POWER() { return getToken(Python3Parser.POWER, 0); }
		public List<TerminalNode> ASSIGN() { return getTokens(Python3Parser.ASSIGN); }
		public TerminalNode ASSIGN(int i) {
			return getToken(Python3Parser.ASSIGN, i);
		}
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public TypedargslistContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typedargslist; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTypedargslist(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTypedargslist(this);
		}
	}

	public final TypedargslistContext typedargslist() throws RecognitionException {
		TypedargslistContext _localctx = new TypedargslistContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_typedargslist);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(385);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MATCH:
			case UNDERSCORE:
			case NAME:
				{
				setState(304);
				tfpdef();
				setState(307);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ASSIGN) {
					{
					setState(305);
					match(ASSIGN);
					setState(306);
					test();
					}
				}

				setState(317);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(309);
						match(COMMA);
						setState(310);
						tfpdef();
						setState(313);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ASSIGN) {
							{
							setState(311);
							match(ASSIGN);
							setState(312);
							test();
							}
						}

						}
						}
					}
					setState(319);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
				}
				setState(353);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(320);
					match(COMMA);
					setState(351);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case STAR:
						{
						setState(321);
						match(STAR);
						setState(323);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 36284957458432L) != 0)) {
							{
							setState(322);
							tfpdef();
							}
						}

						setState(333);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
						while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
							if ( _alt==1 ) {
								{
								{
								setState(325);
								match(COMMA);
								setState(326);
								tfpdef();
								setState(329);
								_errHandler.sync(this);
								_la = _input.LA(1);
								if (_la==ASSIGN) {
									{
									setState(327);
									match(ASSIGN);
									setState(328);
									test();
									}
								}

								}
								}
							}
							setState(335);
							_errHandler.sync(this);
							_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
						}
						setState(344);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(336);
							match(COMMA);
							setState(342);
							_errHandler.sync(this);
							_la = _input.LA(1);
							if (_la==POWER) {
								{
								setState(337);
								match(POWER);
								setState(338);
								tfpdef();
								setState(340);
								_errHandler.sync(this);
								_la = _input.LA(1);
								if (_la==COMMA) {
									{
									setState(339);
									match(COMMA);
									}
								}

								}
							}

							}
						}

						}
						break;
					case POWER:
						{
						setState(346);
						match(POWER);
						setState(347);
						tfpdef();
						setState(349);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(348);
							match(COMMA);
							}
						}

						}
						break;
					case CLOSE_PAREN:
						break;
					default:
						break;
					}
					}
				}

				}
				break;
			case STAR:
				{
				setState(355);
				match(STAR);
				setState(357);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 36284957458432L) != 0)) {
					{
					setState(356);
					tfpdef();
					}
				}

				setState(367);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(359);
						match(COMMA);
						setState(360);
						tfpdef();
						setState(363);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ASSIGN) {
							{
							setState(361);
							match(ASSIGN);
							setState(362);
							test();
							}
						}

						}
						}
					}
					setState(369);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
				}
				setState(378);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(370);
					match(COMMA);
					setState(376);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==POWER) {
						{
						setState(371);
						match(POWER);
						setState(372);
						tfpdef();
						setState(374);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(373);
							match(COMMA);
							}
						}

						}
					}

					}
				}

				}
				break;
			case POWER:
				{
				setState(380);
				match(POWER);
				setState(381);
				tfpdef();
				setState(383);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(382);
					match(COMMA);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TfpdefContext extends ParserRuleContext {
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public TfpdefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tfpdef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTfpdef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTfpdef(this);
		}
	}

	public final TfpdefContext tfpdef() throws RecognitionException {
		TfpdefContext _localctx = new TfpdefContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_tfpdef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(387);
			name();
			setState(390);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COLON) {
				{
				setState(388);
				match(COLON);
				setState(389);
				test();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VarargslistContext extends ParserRuleContext {
		public List<VfpdefContext> vfpdef() {
			return getRuleContexts(VfpdefContext.class);
		}
		public VfpdefContext vfpdef(int i) {
			return getRuleContext(VfpdefContext.class,i);
		}
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public TerminalNode POWER() { return getToken(Python3Parser.POWER, 0); }
		public List<TerminalNode> ASSIGN() { return getTokens(Python3Parser.ASSIGN); }
		public TerminalNode ASSIGN(int i) {
			return getToken(Python3Parser.ASSIGN, i);
		}
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public VarargslistContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varargslist; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterVarargslist(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitVarargslist(this);
		}
	}

	public final VarargslistContext varargslist() throws RecognitionException {
		VarargslistContext _localctx = new VarargslistContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_varargslist);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(473);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MATCH:
			case UNDERSCORE:
			case NAME:
				{
				setState(392);
				vfpdef();
				setState(395);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ASSIGN) {
					{
					setState(393);
					match(ASSIGN);
					setState(394);
					test();
					}
				}

				setState(405);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,33,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(397);
						match(COMMA);
						setState(398);
						vfpdef();
						setState(401);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ASSIGN) {
							{
							setState(399);
							match(ASSIGN);
							setState(400);
							test();
							}
						}

						}
						}
					}
					setState(407);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,33,_ctx);
				}
				setState(441);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(408);
					match(COMMA);
					setState(439);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case STAR:
						{
						setState(409);
						match(STAR);
						setState(411);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 36284957458432L) != 0)) {
							{
							setState(410);
							vfpdef();
							}
						}

						setState(421);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,36,_ctx);
						while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
							if ( _alt==1 ) {
								{
								{
								setState(413);
								match(COMMA);
								setState(414);
								vfpdef();
								setState(417);
								_errHandler.sync(this);
								_la = _input.LA(1);
								if (_la==ASSIGN) {
									{
									setState(415);
									match(ASSIGN);
									setState(416);
									test();
									}
								}

								}
								}
							}
							setState(423);
							_errHandler.sync(this);
							_alt = getInterpreter().adaptivePredict(_input,36,_ctx);
						}
						setState(432);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(424);
							match(COMMA);
							setState(430);
							_errHandler.sync(this);
							_la = _input.LA(1);
							if (_la==POWER) {
								{
								setState(425);
								match(POWER);
								setState(426);
								vfpdef();
								setState(428);
								_errHandler.sync(this);
								_la = _input.LA(1);
								if (_la==COMMA) {
									{
									setState(427);
									match(COMMA);
									}
								}

								}
							}

							}
						}

						}
						break;
					case POWER:
						{
						setState(434);
						match(POWER);
						setState(435);
						vfpdef();
						setState(437);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(436);
							match(COMMA);
							}
						}

						}
						break;
					case COLON:
						break;
					default:
						break;
					}
					}
				}

				}
				break;
			case STAR:
				{
				setState(443);
				match(STAR);
				setState(445);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 36284957458432L) != 0)) {
					{
					setState(444);
					vfpdef();
					}
				}

				setState(455);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,45,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(447);
						match(COMMA);
						setState(448);
						vfpdef();
						setState(451);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ASSIGN) {
							{
							setState(449);
							match(ASSIGN);
							setState(450);
							test();
							}
						}

						}
						}
					}
					setState(457);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,45,_ctx);
				}
				setState(466);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(458);
					match(COMMA);
					setState(464);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==POWER) {
						{
						setState(459);
						match(POWER);
						setState(460);
						vfpdef();
						setState(462);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(461);
							match(COMMA);
							}
						}

						}
					}

					}
				}

				}
				break;
			case POWER:
				{
				setState(468);
				match(POWER);
				setState(469);
				vfpdef();
				setState(471);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(470);
					match(COMMA);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VfpdefContext extends ParserRuleContext {
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public VfpdefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_vfpdef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterVfpdef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitVfpdef(this);
		}
	}

	public final VfpdefContext vfpdef() throws RecognitionException {
		VfpdefContext _localctx = new VfpdefContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_vfpdef);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(475);
			name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StmtContext extends ParserRuleContext {
		public Simple_stmtsContext simple_stmts() {
			return getRuleContext(Simple_stmtsContext.class,0);
		}
		public Compound_stmtContext compound_stmt() {
			return getRuleContext(Compound_stmtContext.class,0);
		}
		public StmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitStmt(this);
		}
	}

	public final StmtContext stmt() throws RecognitionException {
		StmtContext _localctx = new StmtContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_stmt);
		try {
			setState(479);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(477);
				simple_stmts();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(478);
				compound_stmt();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Simple_stmtsContext extends ParserRuleContext {
		public List<Simple_stmtContext> simple_stmt() {
			return getRuleContexts(Simple_stmtContext.class);
		}
		public Simple_stmtContext simple_stmt(int i) {
			return getRuleContext(Simple_stmtContext.class,i);
		}
		public TerminalNode NEWLINE() { return getToken(Python3Parser.NEWLINE, 0); }
		public List<TerminalNode> SEMI_COLON() { return getTokens(Python3Parser.SEMI_COLON); }
		public TerminalNode SEMI_COLON(int i) {
			return getToken(Python3Parser.SEMI_COLON, i);
		}
		public Simple_stmtsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_stmts; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSimple_stmts(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSimple_stmts(this);
		}
	}

	public final Simple_stmtsContext simple_stmts() throws RecognitionException {
		Simple_stmtsContext _localctx = new Simple_stmtsContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_simple_stmts);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(481);
			simple_stmt();
			setState(486);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,52,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(482);
					match(SEMI_COLON);
					setState(483);
					simple_stmt();
					}
					}
				}
				setState(488);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,52,_ctx);
			}
			setState(490);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SEMI_COLON) {
				{
				setState(489);
				match(SEMI_COLON);
				}
			}

			setState(492);
			match(NEWLINE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Simple_stmtContext extends ParserRuleContext {
		public Expr_stmtContext expr_stmt() {
			return getRuleContext(Expr_stmtContext.class,0);
		}
		public Del_stmtContext del_stmt() {
			return getRuleContext(Del_stmtContext.class,0);
		}
		public Pass_stmtContext pass_stmt() {
			return getRuleContext(Pass_stmtContext.class,0);
		}
		public Flow_stmtContext flow_stmt() {
			return getRuleContext(Flow_stmtContext.class,0);
		}
		public Import_stmtContext import_stmt() {
			return getRuleContext(Import_stmtContext.class,0);
		}
		public Global_stmtContext global_stmt() {
			return getRuleContext(Global_stmtContext.class,0);
		}
		public Nonlocal_stmtContext nonlocal_stmt() {
			return getRuleContext(Nonlocal_stmtContext.class,0);
		}
		public Assert_stmtContext assert_stmt() {
			return getRuleContext(Assert_stmtContext.class,0);
		}
		public Simple_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSimple_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSimple_stmt(this);
		}
	}

	public final Simple_stmtContext simple_stmt() throws RecognitionException {
		Simple_stmtContext _localctx = new Simple_stmtContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_simple_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(502);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case LAMBDA:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case STAR:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				{
				setState(494);
				expr_stmt();
				}
				break;
			case DEL:
				{
				setState(495);
				del_stmt();
				}
				break;
			case PASS:
				{
				setState(496);
				pass_stmt();
				}
				break;
			case BREAK:
			case CONTINUE:
			case RAISE:
			case RETURN:
			case YIELD:
				{
				setState(497);
				flow_stmt();
				}
				break;
			case FROM:
			case IMPORT:
				{
				setState(498);
				import_stmt();
				}
				break;
			case GLOBAL:
				{
				setState(499);
				global_stmt();
				}
				break;
			case NONLOCAL:
				{
				setState(500);
				nonlocal_stmt();
				}
				break;
			case ASSERT:
				{
				setState(501);
				assert_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Expr_stmtContext extends ParserRuleContext {
		public List<Testlist_star_exprContext> testlist_star_expr() {
			return getRuleContexts(Testlist_star_exprContext.class);
		}
		public Testlist_star_exprContext testlist_star_expr(int i) {
			return getRuleContext(Testlist_star_exprContext.class,i);
		}
		public AnnassignContext annassign() {
			return getRuleContext(AnnassignContext.class,0);
		}
		public AugassignContext augassign() {
			return getRuleContext(AugassignContext.class,0);
		}
		public List<Yield_exprContext> yield_expr() {
			return getRuleContexts(Yield_exprContext.class);
		}
		public Yield_exprContext yield_expr(int i) {
			return getRuleContext(Yield_exprContext.class,i);
		}
		public TestlistContext testlist() {
			return getRuleContext(TestlistContext.class,0);
		}
		public List<TerminalNode> ASSIGN() { return getTokens(Python3Parser.ASSIGN); }
		public TerminalNode ASSIGN(int i) {
			return getToken(Python3Parser.ASSIGN, i);
		}
		public Expr_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterExpr_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitExpr_stmt(this);
		}
	}

	public final Expr_stmtContext expr_stmt() throws RecognitionException {
		Expr_stmtContext _localctx = new Expr_stmtContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_expr_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(504);
			testlist_star_expr();
			setState(521);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COLON:
				{
				setState(505);
				annassign();
				}
				break;
			case ADD_ASSIGN:
			case SUB_ASSIGN:
			case MULT_ASSIGN:
			case AT_ASSIGN:
			case DIV_ASSIGN:
			case MOD_ASSIGN:
			case AND_ASSIGN:
			case OR_ASSIGN:
			case XOR_ASSIGN:
			case LEFT_SHIFT_ASSIGN:
			case RIGHT_SHIFT_ASSIGN:
			case POWER_ASSIGN:
			case IDIV_ASSIGN:
				{
				setState(506);
				augassign();
				setState(509);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case YIELD:
					{
					setState(507);
					yield_expr();
					}
					break;
				case STRING:
				case NUMBER:
				case AWAIT:
				case FALSE:
				case LAMBDA:
				case MATCH:
				case NONE:
				case NOT:
				case TRUE:
				case UNDERSCORE:
				case NAME:
				case ELLIPSIS:
				case OPEN_PAREN:
				case OPEN_BRACK:
				case ADD:
				case MINUS:
				case NOT_OP:
				case OPEN_BRACE:
					{
					setState(508);
					testlist();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case NEWLINE:
			case SEMI_COLON:
			case ASSIGN:
				{
				setState(518);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==ASSIGN) {
					{
					{
					setState(511);
					match(ASSIGN);
					setState(514);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case YIELD:
						{
						setState(512);
						yield_expr();
						}
						break;
					case STRING:
					case NUMBER:
					case AWAIT:
					case FALSE:
					case LAMBDA:
					case MATCH:
					case NONE:
					case NOT:
					case TRUE:
					case UNDERSCORE:
					case NAME:
					case ELLIPSIS:
					case STAR:
					case OPEN_PAREN:
					case OPEN_BRACK:
					case ADD:
					case MINUS:
					case NOT_OP:
					case OPEN_BRACE:
						{
						setState(513);
						testlist_star_expr();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
					setState(520);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AnnassignContext extends ParserRuleContext {
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public TerminalNode ASSIGN() { return getToken(Python3Parser.ASSIGN, 0); }
		public AnnassignContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_annassign; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAnnassign(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAnnassign(this);
		}
	}

	public final AnnassignContext annassign() throws RecognitionException {
		AnnassignContext _localctx = new AnnassignContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_annassign);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(523);
			match(COLON);
			setState(524);
			test();
			setState(527);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASSIGN) {
				{
				setState(525);
				match(ASSIGN);
				setState(526);
				test();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Testlist_star_exprContext extends ParserRuleContext {
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<Star_exprContext> star_expr() {
			return getRuleContexts(Star_exprContext.class);
		}
		public Star_exprContext star_expr(int i) {
			return getRuleContext(Star_exprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Testlist_star_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_testlist_star_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTestlist_star_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTestlist_star_expr(this);
		}
	}

	public final Testlist_star_exprContext testlist_star_expr() throws RecognitionException {
		Testlist_star_exprContext _localctx = new Testlist_star_exprContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_testlist_star_expr);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(531);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case LAMBDA:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				{
				setState(529);
				test();
				}
				break;
			case STAR:
				{
				setState(530);
				star_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(540);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,62,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(533);
					match(COMMA);
					setState(536);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case STRING:
					case NUMBER:
					case AWAIT:
					case FALSE:
					case LAMBDA:
					case MATCH:
					case NONE:
					case NOT:
					case TRUE:
					case UNDERSCORE:
					case NAME:
					case ELLIPSIS:
					case OPEN_PAREN:
					case OPEN_BRACK:
					case ADD:
					case MINUS:
					case NOT_OP:
					case OPEN_BRACE:
						{
						setState(534);
						test();
						}
						break;
					case STAR:
						{
						setState(535);
						star_expr();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
				}
				setState(542);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,62,_ctx);
			}
			setState(544);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(543);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AugassignContext extends ParserRuleContext {
		public TerminalNode ADD_ASSIGN() { return getToken(Python3Parser.ADD_ASSIGN, 0); }
		public TerminalNode SUB_ASSIGN() { return getToken(Python3Parser.SUB_ASSIGN, 0); }
		public TerminalNode MULT_ASSIGN() { return getToken(Python3Parser.MULT_ASSIGN, 0); }
		public TerminalNode AT_ASSIGN() { return getToken(Python3Parser.AT_ASSIGN, 0); }
		public TerminalNode DIV_ASSIGN() { return getToken(Python3Parser.DIV_ASSIGN, 0); }
		public TerminalNode MOD_ASSIGN() { return getToken(Python3Parser.MOD_ASSIGN, 0); }
		public TerminalNode AND_ASSIGN() { return getToken(Python3Parser.AND_ASSIGN, 0); }
		public TerminalNode OR_ASSIGN() { return getToken(Python3Parser.OR_ASSIGN, 0); }
		public TerminalNode XOR_ASSIGN() { return getToken(Python3Parser.XOR_ASSIGN, 0); }
		public TerminalNode LEFT_SHIFT_ASSIGN() { return getToken(Python3Parser.LEFT_SHIFT_ASSIGN, 0); }
		public TerminalNode RIGHT_SHIFT_ASSIGN() { return getToken(Python3Parser.RIGHT_SHIFT_ASSIGN, 0); }
		public TerminalNode POWER_ASSIGN() { return getToken(Python3Parser.POWER_ASSIGN, 0); }
		public TerminalNode IDIV_ASSIGN() { return getToken(Python3Parser.IDIV_ASSIGN, 0); }
		public AugassignContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_augassign; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAugassign(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAugassign(this);
		}
	}

	public final AugassignContext augassign() throws RecognitionException {
		AugassignContext _localctx = new AugassignContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_augassign);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(546);
			_la = _input.LA(1);
			if ( !(((((_la - 88)) & ~0x3f) == 0 && ((1L << (_la - 88)) & 8191L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Del_stmtContext extends ParserRuleContext {
		public TerminalNode DEL() { return getToken(Python3Parser.DEL, 0); }
		public ExprlistContext exprlist() {
			return getRuleContext(ExprlistContext.class,0);
		}
		public Del_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_del_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDel_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDel_stmt(this);
		}
	}

	public final Del_stmtContext del_stmt() throws RecognitionException {
		Del_stmtContext _localctx = new Del_stmtContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_del_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(548);
			match(DEL);
			setState(549);
			exprlist();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Pass_stmtContext extends ParserRuleContext {
		public TerminalNode PASS() { return getToken(Python3Parser.PASS, 0); }
		public Pass_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pass_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterPass_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitPass_stmt(this);
		}
	}

	public final Pass_stmtContext pass_stmt() throws RecognitionException {
		Pass_stmtContext _localctx = new Pass_stmtContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_pass_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(551);
			match(PASS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Flow_stmtContext extends ParserRuleContext {
		public Break_stmtContext break_stmt() {
			return getRuleContext(Break_stmtContext.class,0);
		}
		public Continue_stmtContext continue_stmt() {
			return getRuleContext(Continue_stmtContext.class,0);
		}
		public Return_stmtContext return_stmt() {
			return getRuleContext(Return_stmtContext.class,0);
		}
		public Raise_stmtContext raise_stmt() {
			return getRuleContext(Raise_stmtContext.class,0);
		}
		public Yield_stmtContext yield_stmt() {
			return getRuleContext(Yield_stmtContext.class,0);
		}
		public Flow_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_flow_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterFlow_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitFlow_stmt(this);
		}
	}

	public final Flow_stmtContext flow_stmt() throws RecognitionException {
		Flow_stmtContext _localctx = new Flow_stmtContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_flow_stmt);
		try {
			setState(558);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BREAK:
				enterOuterAlt(_localctx, 1);
				{
				setState(553);
				break_stmt();
				}
				break;
			case CONTINUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(554);
				continue_stmt();
				}
				break;
			case RETURN:
				enterOuterAlt(_localctx, 3);
				{
				setState(555);
				return_stmt();
				}
				break;
			case RAISE:
				enterOuterAlt(_localctx, 4);
				{
				setState(556);
				raise_stmt();
				}
				break;
			case YIELD:
				enterOuterAlt(_localctx, 5);
				{
				setState(557);
				yield_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Break_stmtContext extends ParserRuleContext {
		public TerminalNode BREAK() { return getToken(Python3Parser.BREAK, 0); }
		public Break_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_break_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterBreak_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitBreak_stmt(this);
		}
	}

	public final Break_stmtContext break_stmt() throws RecognitionException {
		Break_stmtContext _localctx = new Break_stmtContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_break_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(560);
			match(BREAK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Continue_stmtContext extends ParserRuleContext {
		public TerminalNode CONTINUE() { return getToken(Python3Parser.CONTINUE, 0); }
		public Continue_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_continue_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterContinue_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitContinue_stmt(this);
		}
	}

	public final Continue_stmtContext continue_stmt() throws RecognitionException {
		Continue_stmtContext _localctx = new Continue_stmtContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_continue_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(562);
			match(CONTINUE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Return_stmtContext extends ParserRuleContext {
		public TerminalNode RETURN() { return getToken(Python3Parser.RETURN, 0); }
		public TestlistContext testlist() {
			return getRuleContext(TestlistContext.class,0);
		}
		public Return_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_return_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterReturn_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitReturn_stmt(this);
		}
	}

	public final Return_stmtContext return_stmt() throws RecognitionException {
		Return_stmtContext _localctx = new Return_stmtContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_return_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(564);
			match(RETURN);
			setState(566);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556205523992L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
				{
				setState(565);
				testlist();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Yield_stmtContext extends ParserRuleContext {
		public Yield_exprContext yield_expr() {
			return getRuleContext(Yield_exprContext.class,0);
		}
		public Yield_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_yield_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterYield_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitYield_stmt(this);
		}
	}

	public final Yield_stmtContext yield_stmt() throws RecognitionException {
		Yield_stmtContext _localctx = new Yield_stmtContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_yield_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568);
			yield_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Raise_stmtContext extends ParserRuleContext {
		public TerminalNode RAISE() { return getToken(Python3Parser.RAISE, 0); }
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public TerminalNode FROM() { return getToken(Python3Parser.FROM, 0); }
		public Raise_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_raise_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterRaise_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitRaise_stmt(this);
		}
	}

	public final Raise_stmtContext raise_stmt() throws RecognitionException {
		Raise_stmtContext _localctx = new Raise_stmtContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_raise_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(570);
			match(RAISE);
			setState(576);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556205523992L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
				{
				setState(571);
				test();
				setState(574);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM) {
					{
					setState(572);
					match(FROM);
					setState(573);
					test();
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Import_stmtContext extends ParserRuleContext {
		public Import_nameContext import_name() {
			return getRuleContext(Import_nameContext.class,0);
		}
		public Import_fromContext import_from() {
			return getRuleContext(Import_fromContext.class,0);
		}
		public Import_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_import_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterImport_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitImport_stmt(this);
		}
	}

	public final Import_stmtContext import_stmt() throws RecognitionException {
		Import_stmtContext _localctx = new Import_stmtContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_import_stmt);
		try {
			setState(580);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IMPORT:
				enterOuterAlt(_localctx, 1);
				{
				setState(578);
				import_name();
				}
				break;
			case FROM:
				enterOuterAlt(_localctx, 2);
				{
				setState(579);
				import_from();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Import_nameContext extends ParserRuleContext {
		public TerminalNode IMPORT() { return getToken(Python3Parser.IMPORT, 0); }
		public Dotted_as_namesContext dotted_as_names() {
			return getRuleContext(Dotted_as_namesContext.class,0);
		}
		public Import_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_import_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterImport_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitImport_name(this);
		}
	}

	public final Import_nameContext import_name() throws RecognitionException {
		Import_nameContext _localctx = new Import_nameContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_import_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(582);
			match(IMPORT);
			setState(583);
			dotted_as_names();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Import_fromContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(Python3Parser.FROM, 0); }
		public TerminalNode IMPORT() { return getToken(Python3Parser.IMPORT, 0); }
		public Dotted_nameContext dotted_name() {
			return getRuleContext(Dotted_nameContext.class,0);
		}
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public Import_as_namesContext import_as_names() {
			return getRuleContext(Import_as_namesContext.class,0);
		}
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public List<TerminalNode> DOT() { return getTokens(Python3Parser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(Python3Parser.DOT, i);
		}
		public List<TerminalNode> ELLIPSIS() { return getTokens(Python3Parser.ELLIPSIS); }
		public TerminalNode ELLIPSIS(int i) {
			return getToken(Python3Parser.ELLIPSIS, i);
		}
		public Import_fromContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_import_from; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterImport_from(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitImport_from(this);
		}
	}

	public final Import_fromContext import_from() throws RecognitionException {
		Import_fromContext _localctx = new Import_fromContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_import_from);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(585);
			match(FROM);
			setState(598);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
			case 1:
				{
				setState(589);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==DOT || _la==ELLIPSIS) {
					{
					{
					setState(586);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==ELLIPSIS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(591);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(592);
				dotted_name();
				}
				break;
			case 2:
				{
				setState(594);
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(593);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==ELLIPSIS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(596);
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==ELLIPSIS );
				}
				break;
			}
			setState(600);
			match(IMPORT);
			setState(607);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(601);
				match(STAR);
				}
				break;
			case OPEN_PAREN:
				{
				setState(602);
				match(OPEN_PAREN);
				setState(603);
				import_as_names();
				setState(604);
				match(CLOSE_PAREN);
				}
				break;
			case MATCH:
			case UNDERSCORE:
			case NAME:
				{
				setState(606);
				import_as_names();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Import_as_nameContext extends ParserRuleContext {
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public TerminalNode AS() { return getToken(Python3Parser.AS, 0); }
		public Import_as_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_import_as_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterImport_as_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitImport_as_name(this);
		}
	}

	public final Import_as_nameContext import_as_name() throws RecognitionException {
		Import_as_nameContext _localctx = new Import_as_nameContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_import_as_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(609);
			name();
			setState(612);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(610);
				match(AS);
				setState(611);
				name();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Dotted_as_nameContext extends ParserRuleContext {
		public Dotted_nameContext dotted_name() {
			return getRuleContext(Dotted_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(Python3Parser.AS, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Dotted_as_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dotted_as_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDotted_as_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDotted_as_name(this);
		}
	}

	public final Dotted_as_nameContext dotted_as_name() throws RecognitionException {
		Dotted_as_nameContext _localctx = new Dotted_as_nameContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_dotted_as_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(614);
			dotted_name();
			setState(617);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(615);
				match(AS);
				setState(616);
				name();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Import_as_namesContext extends ParserRuleContext {
		public List<Import_as_nameContext> import_as_name() {
			return getRuleContexts(Import_as_nameContext.class);
		}
		public Import_as_nameContext import_as_name(int i) {
			return getRuleContext(Import_as_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Import_as_namesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_import_as_names; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterImport_as_names(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitImport_as_names(this);
		}
	}

	public final Import_as_namesContext import_as_names() throws RecognitionException {
		Import_as_namesContext _localctx = new Import_as_namesContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_import_as_names);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(619);
			import_as_name();
			setState(624);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(620);
					match(COMMA);
					setState(621);
					import_as_name();
					}
					}
				}
				setState(626);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			}
			setState(628);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(627);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Dotted_as_namesContext extends ParserRuleContext {
		public List<Dotted_as_nameContext> dotted_as_name() {
			return getRuleContexts(Dotted_as_nameContext.class);
		}
		public Dotted_as_nameContext dotted_as_name(int i) {
			return getRuleContext(Dotted_as_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Dotted_as_namesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dotted_as_names; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDotted_as_names(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDotted_as_names(this);
		}
	}

	public final Dotted_as_namesContext dotted_as_names() throws RecognitionException {
		Dotted_as_namesContext _localctx = new Dotted_as_namesContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_dotted_as_names);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(630);
			dotted_as_name();
			setState(635);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(631);
				match(COMMA);
				setState(632);
				dotted_as_name();
				}
				}
				setState(637);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Dotted_nameContext extends ParserRuleContext {
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(Python3Parser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(Python3Parser.DOT, i);
		}
		public Dotted_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dotted_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDotted_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDotted_name(this);
		}
	}

	public final Dotted_nameContext dotted_name() throws RecognitionException {
		Dotted_nameContext _localctx = new Dotted_nameContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_dotted_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(638);
			name();
			setState(643);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(639);
				match(DOT);
				setState(640);
				name();
				}
				}
				setState(645);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Global_stmtContext extends ParserRuleContext {
		public TerminalNode GLOBAL() { return getToken(Python3Parser.GLOBAL, 0); }
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Global_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_global_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterGlobal_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitGlobal_stmt(this);
		}
	}

	public final Global_stmtContext global_stmt() throws RecognitionException {
		Global_stmtContext _localctx = new Global_stmtContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_global_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(646);
			match(GLOBAL);
			setState(647);
			name();
			setState(652);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(648);
				match(COMMA);
				setState(649);
				name();
				}
				}
				setState(654);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Nonlocal_stmtContext extends ParserRuleContext {
		public TerminalNode NONLOCAL() { return getToken(Python3Parser.NONLOCAL, 0); }
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Nonlocal_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonlocal_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterNonlocal_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitNonlocal_stmt(this);
		}
	}

	public final Nonlocal_stmtContext nonlocal_stmt() throws RecognitionException {
		Nonlocal_stmtContext _localctx = new Nonlocal_stmtContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_nonlocal_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(655);
			match(NONLOCAL);
			setState(656);
			name();
			setState(661);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(657);
				match(COMMA);
				setState(658);
				name();
				}
				}
				setState(663);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Assert_stmtContext extends ParserRuleContext {
		public TerminalNode ASSERT() { return getToken(Python3Parser.ASSERT, 0); }
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public TerminalNode COMMA() { return getToken(Python3Parser.COMMA, 0); }
		public Assert_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assert_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAssert_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAssert_stmt(this);
		}
	}

	public final Assert_stmtContext assert_stmt() throws RecognitionException {
		Assert_stmtContext _localctx = new Assert_stmtContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_assert_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(664);
			match(ASSERT);
			setState(665);
			test();
			setState(668);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(666);
				match(COMMA);
				setState(667);
				test();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Compound_stmtContext extends ParserRuleContext {
		public If_stmtContext if_stmt() {
			return getRuleContext(If_stmtContext.class,0);
		}
		public While_stmtContext while_stmt() {
			return getRuleContext(While_stmtContext.class,0);
		}
		public For_stmtContext for_stmt() {
			return getRuleContext(For_stmtContext.class,0);
		}
		public Try_stmtContext try_stmt() {
			return getRuleContext(Try_stmtContext.class,0);
		}
		public With_stmtContext with_stmt() {
			return getRuleContext(With_stmtContext.class,0);
		}
		public FuncdefContext funcdef() {
			return getRuleContext(FuncdefContext.class,0);
		}
		public ClassdefContext classdef() {
			return getRuleContext(ClassdefContext.class,0);
		}
		public DecoratedContext decorated() {
			return getRuleContext(DecoratedContext.class,0);
		}
		public Async_stmtContext async_stmt() {
			return getRuleContext(Async_stmtContext.class,0);
		}
		public Match_stmtContext match_stmt() {
			return getRuleContext(Match_stmtContext.class,0);
		}
		public Compound_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compound_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterCompound_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitCompound_stmt(this);
		}
	}

	public final Compound_stmtContext compound_stmt() throws RecognitionException {
		Compound_stmtContext _localctx = new Compound_stmtContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_compound_stmt);
		try {
			setState(680);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IF:
				enterOuterAlt(_localctx, 1);
				{
				setState(670);
				if_stmt();
				}
				break;
			case WHILE:
				enterOuterAlt(_localctx, 2);
				{
				setState(671);
				while_stmt();
				}
				break;
			case FOR:
				enterOuterAlt(_localctx, 3);
				{
				setState(672);
				for_stmt();
				}
				break;
			case TRY:
				enterOuterAlt(_localctx, 4);
				{
				setState(673);
				try_stmt();
				}
				break;
			case WITH:
				enterOuterAlt(_localctx, 5);
				{
				setState(674);
				with_stmt();
				}
				break;
			case DEF:
				enterOuterAlt(_localctx, 6);
				{
				setState(675);
				funcdef();
				}
				break;
			case CLASS:
				enterOuterAlt(_localctx, 7);
				{
				setState(676);
				classdef();
				}
				break;
			case AT:
				enterOuterAlt(_localctx, 8);
				{
				setState(677);
				decorated();
				}
				break;
			case ASYNC:
				enterOuterAlt(_localctx, 9);
				{
				setState(678);
				async_stmt();
				}
				break;
			case MATCH:
				enterOuterAlt(_localctx, 10);
				{
				setState(679);
				match_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Async_stmtContext extends ParserRuleContext {
		public TerminalNode ASYNC() { return getToken(Python3Parser.ASYNC, 0); }
		public FuncdefContext funcdef() {
			return getRuleContext(FuncdefContext.class,0);
		}
		public With_stmtContext with_stmt() {
			return getRuleContext(With_stmtContext.class,0);
		}
		public For_stmtContext for_stmt() {
			return getRuleContext(For_stmtContext.class,0);
		}
		public Async_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_async_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAsync_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAsync_stmt(this);
		}
	}

	public final Async_stmtContext async_stmt() throws RecognitionException {
		Async_stmtContext _localctx = new Async_stmtContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_async_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(682);
			match(ASYNC);
			setState(686);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEF:
				{
				setState(683);
				funcdef();
				}
				break;
			case WITH:
				{
				setState(684);
				with_stmt();
				}
				break;
			case FOR:
				{
				setState(685);
				for_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class If_stmtContext extends ParserRuleContext {
		public TerminalNode IF() { return getToken(Python3Parser.IF, 0); }
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<TerminalNode> COLON() { return getTokens(Python3Parser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(Python3Parser.COLON, i);
		}
		public List<BlockContext> block() {
			return getRuleContexts(BlockContext.class);
		}
		public BlockContext block(int i) {
			return getRuleContext(BlockContext.class,i);
		}
		public List<TerminalNode> ELIF() { return getTokens(Python3Parser.ELIF); }
		public TerminalNode ELIF(int i) {
			return getToken(Python3Parser.ELIF, i);
		}
		public TerminalNode ELSE() { return getToken(Python3Parser.ELSE, 0); }
		public If_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_if_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterIf_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitIf_stmt(this);
		}
	}

	public final If_stmtContext if_stmt() throws RecognitionException {
		If_stmtContext _localctx = new If_stmtContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_if_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(688);
			match(IF);
			setState(689);
			test();
			setState(690);
			match(COLON);
			setState(691);
			block();
			setState(699);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ELIF) {
				{
				{
				setState(692);
				match(ELIF);
				setState(693);
				test();
				setState(694);
				match(COLON);
				setState(695);
				block();
				}
				}
				setState(701);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(705);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(702);
				match(ELSE);
				setState(703);
				match(COLON);
				setState(704);
				block();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class While_stmtContext extends ParserRuleContext {
		public TerminalNode WHILE() { return getToken(Python3Parser.WHILE, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public List<TerminalNode> COLON() { return getTokens(Python3Parser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(Python3Parser.COLON, i);
		}
		public List<BlockContext> block() {
			return getRuleContexts(BlockContext.class);
		}
		public BlockContext block(int i) {
			return getRuleContext(BlockContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(Python3Parser.ELSE, 0); }
		public While_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_while_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterWhile_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitWhile_stmt(this);
		}
	}

	public final While_stmtContext while_stmt() throws RecognitionException {
		While_stmtContext _localctx = new While_stmtContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_while_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(707);
			match(WHILE);
			setState(708);
			test();
			setState(709);
			match(COLON);
			setState(710);
			block();
			setState(714);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(711);
				match(ELSE);
				setState(712);
				match(COLON);
				setState(713);
				block();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class For_stmtContext extends ParserRuleContext {
		public TerminalNode FOR() { return getToken(Python3Parser.FOR, 0); }
		public ExprlistContext exprlist() {
			return getRuleContext(ExprlistContext.class,0);
		}
		public TerminalNode IN() { return getToken(Python3Parser.IN, 0); }
		public TestlistContext testlist() {
			return getRuleContext(TestlistContext.class,0);
		}
		public List<TerminalNode> COLON() { return getTokens(Python3Parser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(Python3Parser.COLON, i);
		}
		public List<BlockContext> block() {
			return getRuleContexts(BlockContext.class);
		}
		public BlockContext block(int i) {
			return getRuleContext(BlockContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(Python3Parser.ELSE, 0); }
		public For_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_for_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterFor_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitFor_stmt(this);
		}
	}

	public final For_stmtContext for_stmt() throws RecognitionException {
		For_stmtContext _localctx = new For_stmtContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_for_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(716);
			match(FOR);
			setState(717);
			exprlist();
			setState(718);
			match(IN);
			setState(719);
			testlist();
			setState(720);
			match(COLON);
			setState(721);
			block();
			setState(725);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(722);
				match(ELSE);
				setState(723);
				match(COLON);
				setState(724);
				block();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Try_stmtContext extends ParserRuleContext {
		public TerminalNode TRY() { return getToken(Python3Parser.TRY, 0); }
		public List<TerminalNode> COLON() { return getTokens(Python3Parser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(Python3Parser.COLON, i);
		}
		public List<BlockContext> block() {
			return getRuleContexts(BlockContext.class);
		}
		public BlockContext block(int i) {
			return getRuleContext(BlockContext.class,i);
		}
		public TerminalNode FINALLY() { return getToken(Python3Parser.FINALLY, 0); }
		public List<Except_clauseContext> except_clause() {
			return getRuleContexts(Except_clauseContext.class);
		}
		public Except_clauseContext except_clause(int i) {
			return getRuleContext(Except_clauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(Python3Parser.ELSE, 0); }
		public Try_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_try_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTry_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTry_stmt(this);
		}
	}

	public final Try_stmtContext try_stmt() throws RecognitionException {
		Try_stmtContext _localctx = new Try_stmtContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_try_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(727);
			match(TRY);
			setState(728);
			match(COLON);
			setState(729);
			block();
			setState(751);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EXCEPT:
				{
				setState(734);
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(730);
					except_clause();
					setState(731);
					match(COLON);
					setState(732);
					block();
					}
					}
					setState(736);
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==EXCEPT );
				setState(741);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(738);
					match(ELSE);
					setState(739);
					match(COLON);
					setState(740);
					block();
					}
				}

				setState(746);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FINALLY) {
					{
					setState(743);
					match(FINALLY);
					setState(744);
					match(COLON);
					setState(745);
					block();
					}
				}

				}
				break;
			case FINALLY:
				{
				setState(748);
				match(FINALLY);
				setState(749);
				match(COLON);
				setState(750);
				block();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class With_stmtContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(Python3Parser.WITH, 0); }
		public List<With_itemContext> with_item() {
			return getRuleContexts(With_itemContext.class);
		}
		public With_itemContext with_item(int i) {
			return getRuleContext(With_itemContext.class,i);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public BlockContext block() {
			return getRuleContext(BlockContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public With_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterWith_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitWith_stmt(this);
		}
	}

	public final With_stmtContext with_stmt() throws RecognitionException {
		With_stmtContext _localctx = new With_stmtContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_with_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(753);
			match(WITH);
			setState(754);
			with_item();
			setState(759);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(755);
				match(COMMA);
				setState(756);
				with_item();
				}
				}
				setState(761);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(762);
			match(COLON);
			setState(763);
			block();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class With_itemContext extends ParserRuleContext {
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public TerminalNode AS() { return getToken(Python3Parser.AS, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public With_itemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_item; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterWith_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitWith_item(this);
		}
	}

	public final With_itemContext with_item() throws RecognitionException {
		With_itemContext _localctx = new With_itemContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_with_item);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(765);
			test();
			setState(768);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(766);
				match(AS);
				setState(767);
				expr(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Except_clauseContext extends ParserRuleContext {
		public TerminalNode EXCEPT() { return getToken(Python3Parser.EXCEPT, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public TerminalNode AS() { return getToken(Python3Parser.AS, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Except_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_except_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterExcept_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitExcept_clause(this);
		}
	}

	public final Except_clauseContext except_clause() throws RecognitionException {
		Except_clauseContext _localctx = new Except_clauseContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_except_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(770);
			match(EXCEPT);
			setState(776);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556205523992L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
				{
				setState(771);
				test();
				setState(774);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(772);
					match(AS);
					setState(773);
					name();
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BlockContext extends ParserRuleContext {
		public Simple_stmtsContext simple_stmts() {
			return getRuleContext(Simple_stmtsContext.class,0);
		}
		public TerminalNode NEWLINE() { return getToken(Python3Parser.NEWLINE, 0); }
		public TerminalNode INDENT() { return getToken(Python3Parser.INDENT, 0); }
		public TerminalNode DEDENT() { return getToken(Python3Parser.DEDENT, 0); }
		public List<StmtContext> stmt() {
			return getRuleContexts(StmtContext.class);
		}
		public StmtContext stmt(int i) {
			return getRuleContext(StmtContext.class,i);
		}
		public BlockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterBlock(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitBlock(this);
		}
	}

	public final BlockContext block() throws RecognitionException {
		BlockContext _localctx = new BlockContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_block);
		int _la;
		try {
			setState(788);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case ASSERT:
			case AWAIT:
			case BREAK:
			case CONTINUE:
			case DEL:
			case FALSE:
			case FROM:
			case GLOBAL:
			case IMPORT:
			case LAMBDA:
			case MATCH:
			case NONE:
			case NONLOCAL:
			case NOT:
			case PASS:
			case RAISE:
			case RETURN:
			case TRUE:
			case UNDERSCORE:
			case YIELD:
			case NAME:
			case ELLIPSIS:
			case STAR:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 1);
				{
				setState(778);
				simple_stmts();
				}
				break;
			case NEWLINE:
				enterOuterAlt(_localctx, 2);
				{
				setState(779);
				match(NEWLINE);
				setState(780);
				match(INDENT);
				setState(782);
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(781);
					stmt();
					}
					}
					setState(784);
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 252254338105339672L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 4206977L) != 0) );
				setState(786);
				match(DEDENT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Match_stmtContext extends ParserRuleContext {
		public TerminalNode MATCH() { return getToken(Python3Parser.MATCH, 0); }
		public Subject_exprContext subject_expr() {
			return getRuleContext(Subject_exprContext.class,0);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public TerminalNode NEWLINE() { return getToken(Python3Parser.NEWLINE, 0); }
		public TerminalNode INDENT() { return getToken(Python3Parser.INDENT, 0); }
		public TerminalNode DEDENT() { return getToken(Python3Parser.DEDENT, 0); }
		public List<Case_blockContext> case_block() {
			return getRuleContexts(Case_blockContext.class);
		}
		public Case_blockContext case_block(int i) {
			return getRuleContext(Case_blockContext.class,i);
		}
		public Match_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_match_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterMatch_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitMatch_stmt(this);
		}
	}

	public final Match_stmtContext match_stmt() throws RecognitionException {
		Match_stmtContext _localctx = new Match_stmtContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_match_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(790);
			match(MATCH);
			setState(791);
			subject_expr();
			setState(792);
			match(COLON);
			setState(793);
			match(NEWLINE);
			setState(794);
			match(INDENT);
			setState(796);
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(795);
				case_block();
				}
				}
				setState(798);
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==CASE );
			setState(800);
			match(DEDENT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Subject_exprContext extends ParserRuleContext {
		public Star_named_expressionContext star_named_expression() {
			return getRuleContext(Star_named_expressionContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(Python3Parser.COMMA, 0); }
		public Star_named_expressionsContext star_named_expressions() {
			return getRuleContext(Star_named_expressionsContext.class,0);
		}
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public Subject_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subject_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSubject_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSubject_expr(this);
		}
	}

	public final Subject_exprContext subject_expr() throws RecognitionException {
		Subject_exprContext _localctx = new Subject_exprContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_subject_expr);
		int _la;
		try {
			setState(808);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(802);
				star_named_expression();
				setState(803);
				match(COMMA);
				setState(805);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(804);
					star_named_expressions();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(807);
				test();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Star_named_expressionsContext extends ParserRuleContext {
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public List<Star_named_expressionContext> star_named_expression() {
			return getRuleContexts(Star_named_expressionContext.class);
		}
		public Star_named_expressionContext star_named_expression(int i) {
			return getRuleContext(Star_named_expressionContext.class,i);
		}
		public Star_named_expressionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_star_named_expressions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterStar_named_expressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitStar_named_expressions(this);
		}
	}

	public final Star_named_expressionsContext star_named_expressions() throws RecognitionException {
		Star_named_expressionsContext _localctx = new Star_named_expressionsContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_star_named_expressions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(810);
			match(COMMA);
			setState(812);
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(811);
				star_named_expression();
				}
				}
				setState(814);
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 252238150243451928L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0) );
			setState(817);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(816);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Star_named_expressionContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public Star_named_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_star_named_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterStar_named_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitStar_named_expression(this);
		}
	}

	public final Star_named_expressionContext star_named_expression() throws RecognitionException {
		Star_named_expressionContext _localctx = new Star_named_expressionContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_star_named_expression);
		try {
			setState(822);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(819);
				match(STAR);
				setState(820);
				expr(0);
				}
				break;
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case LAMBDA:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 2);
				{
				setState(821);
				test();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Case_blockContext extends ParserRuleContext {
		public TerminalNode CASE() { return getToken(Python3Parser.CASE, 0); }
		public PatternsContext patterns() {
			return getRuleContext(PatternsContext.class,0);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public BlockContext block() {
			return getRuleContext(BlockContext.class,0);
		}
		public GuardContext guard() {
			return getRuleContext(GuardContext.class,0);
		}
		public Case_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_case_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterCase_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitCase_block(this);
		}
	}

	public final Case_blockContext case_block() throws RecognitionException {
		Case_blockContext _localctx = new Case_blockContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_case_block);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(824);
			match(CASE);
			setState(825);
			patterns();
			setState(827);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IF) {
				{
				setState(826);
				guard();
				}
			}

			setState(829);
			match(COLON);
			setState(830);
			block();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GuardContext extends ParserRuleContext {
		public TerminalNode IF() { return getToken(Python3Parser.IF, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public GuardContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_guard; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterGuard(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitGuard(this);
		}
	}

	public final GuardContext guard() throws RecognitionException {
		GuardContext _localctx = new GuardContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_guard);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(832);
			match(IF);
			setState(833);
			test();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PatternsContext extends ParserRuleContext {
		public Open_sequence_patternContext open_sequence_pattern() {
			return getRuleContext(Open_sequence_patternContext.class,0);
		}
		public PatternContext pattern() {
			return getRuleContext(PatternContext.class,0);
		}
		public PatternsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patterns; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterPatterns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitPatterns(this);
		}
	}

	public final PatternsContext patterns() throws RecognitionException {
		PatternsContext _localctx = new PatternsContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_patterns);
		try {
			setState(837);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,105,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(835);
				open_sequence_pattern();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(836);
				pattern();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PatternContext extends ParserRuleContext {
		public As_patternContext as_pattern() {
			return getRuleContext(As_patternContext.class,0);
		}
		public Or_patternContext or_pattern() {
			return getRuleContext(Or_patternContext.class,0);
		}
		public PatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitPattern(this);
		}
	}

	public final PatternContext pattern() throws RecognitionException {
		PatternContext _localctx = new PatternContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_pattern);
		try {
			setState(841);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,106,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(839);
				as_pattern();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(840);
				or_pattern();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class As_patternContext extends ParserRuleContext {
		public Or_patternContext or_pattern() {
			return getRuleContext(Or_patternContext.class,0);
		}
		public TerminalNode AS() { return getToken(Python3Parser.AS, 0); }
		public Pattern_capture_targetContext pattern_capture_target() {
			return getRuleContext(Pattern_capture_targetContext.class,0);
		}
		public As_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_as_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAs_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAs_pattern(this);
		}
	}

	public final As_patternContext as_pattern() throws RecognitionException {
		As_patternContext _localctx = new As_patternContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_as_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(843);
			or_pattern();
			setState(844);
			match(AS);
			setState(845);
			pattern_capture_target();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Or_patternContext extends ParserRuleContext {
		public List<Closed_patternContext> closed_pattern() {
			return getRuleContexts(Closed_patternContext.class);
		}
		public Closed_patternContext closed_pattern(int i) {
			return getRuleContext(Closed_patternContext.class,i);
		}
		public List<TerminalNode> OR_OP() { return getTokens(Python3Parser.OR_OP); }
		public TerminalNode OR_OP(int i) {
			return getToken(Python3Parser.OR_OP, i);
		}
		public Or_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_or_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterOr_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitOr_pattern(this);
		}
	}

	public final Or_patternContext or_pattern() throws RecognitionException {
		Or_patternContext _localctx = new Or_patternContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_or_pattern);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(847);
			closed_pattern();
			setState(852);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OR_OP) {
				{
				{
				setState(848);
				match(OR_OP);
				setState(849);
				closed_pattern();
				}
				}
				setState(854);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Closed_patternContext extends ParserRuleContext {
		public Literal_patternContext literal_pattern() {
			return getRuleContext(Literal_patternContext.class,0);
		}
		public Capture_patternContext capture_pattern() {
			return getRuleContext(Capture_patternContext.class,0);
		}
		public Wildcard_patternContext wildcard_pattern() {
			return getRuleContext(Wildcard_patternContext.class,0);
		}
		public Value_patternContext value_pattern() {
			return getRuleContext(Value_patternContext.class,0);
		}
		public Group_patternContext group_pattern() {
			return getRuleContext(Group_patternContext.class,0);
		}
		public Sequence_patternContext sequence_pattern() {
			return getRuleContext(Sequence_patternContext.class,0);
		}
		public Mapping_patternContext mapping_pattern() {
			return getRuleContext(Mapping_patternContext.class,0);
		}
		public Class_patternContext class_pattern() {
			return getRuleContext(Class_patternContext.class,0);
		}
		public Closed_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_closed_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterClosed_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitClosed_pattern(this);
		}
	}

	public final Closed_patternContext closed_pattern() throws RecognitionException {
		Closed_patternContext _localctx = new Closed_patternContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_closed_pattern);
		try {
			setState(863);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(855);
				literal_pattern();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(856);
				capture_pattern();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(857);
				wildcard_pattern();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(858);
				value_pattern();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(859);
				group_pattern();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(860);
				sequence_pattern();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(861);
				mapping_pattern();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(862);
				class_pattern();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Literal_patternContext extends ParserRuleContext {
		public Signed_numberContext signed_number() {
			return getRuleContext(Signed_numberContext.class,0);
		}
		public Complex_numberContext complex_number() {
			return getRuleContext(Complex_numberContext.class,0);
		}
		public StringsContext strings() {
			return getRuleContext(StringsContext.class,0);
		}
		public TerminalNode NONE() { return getToken(Python3Parser.NONE, 0); }
		public TerminalNode TRUE() { return getToken(Python3Parser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(Python3Parser.FALSE, 0); }
		public Literal_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterLiteral_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitLiteral_pattern(this);
		}
	}

	public final Literal_patternContext literal_pattern() throws RecognitionException {
		Literal_patternContext _localctx = new Literal_patternContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_literal_pattern);
		try {
			setState(873);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(865);
				signed_number();
				setState(866);
				if (!( this.CannotBePlusMinus() )) throw new FailedPredicateException(this, " this.CannotBePlusMinus() ");
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(868);
				complex_number();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(869);
				strings();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(870);
				match(NONE);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(871);
				match(TRUE);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(872);
				match(FALSE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Literal_exprContext extends ParserRuleContext {
		public Signed_numberContext signed_number() {
			return getRuleContext(Signed_numberContext.class,0);
		}
		public Complex_numberContext complex_number() {
			return getRuleContext(Complex_numberContext.class,0);
		}
		public StringsContext strings() {
			return getRuleContext(StringsContext.class,0);
		}
		public TerminalNode NONE() { return getToken(Python3Parser.NONE, 0); }
		public TerminalNode TRUE() { return getToken(Python3Parser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(Python3Parser.FALSE, 0); }
		public Literal_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterLiteral_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitLiteral_expr(this);
		}
	}

	public final Literal_exprContext literal_expr() throws RecognitionException {
		Literal_exprContext _localctx = new Literal_exprContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_literal_expr);
		try {
			setState(883);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,110,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(875);
				signed_number();
				setState(876);
				if (!( this.CannotBePlusMinus() )) throw new FailedPredicateException(this, " this.CannotBePlusMinus() ");
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(878);
				complex_number();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(879);
				strings();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(880);
				match(NONE);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(881);
				match(TRUE);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(882);
				match(FALSE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Complex_numberContext extends ParserRuleContext {
		public Signed_real_numberContext signed_real_number() {
			return getRuleContext(Signed_real_numberContext.class,0);
		}
		public TerminalNode ADD() { return getToken(Python3Parser.ADD, 0); }
		public Imaginary_numberContext imaginary_number() {
			return getRuleContext(Imaginary_numberContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(Python3Parser.MINUS, 0); }
		public Complex_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complex_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterComplex_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitComplex_number(this);
		}
	}

	public final Complex_numberContext complex_number() throws RecognitionException {
		Complex_numberContext _localctx = new Complex_numberContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_complex_number);
		try {
			setState(893);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(885);
				signed_real_number();
				setState(886);
				match(ADD);
				setState(887);
				imaginary_number();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(889);
				signed_real_number();
				setState(890);
				match(MINUS);
				setState(891);
				imaginary_number();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Signed_numberContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(Python3Parser.NUMBER, 0); }
		public TerminalNode MINUS() { return getToken(Python3Parser.MINUS, 0); }
		public Signed_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signed_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSigned_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSigned_number(this);
		}
	}

	public final Signed_numberContext signed_number() throws RecognitionException {
		Signed_numberContext _localctx = new Signed_numberContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_signed_number);
		try {
			setState(898);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NUMBER:
				enterOuterAlt(_localctx, 1);
				{
				setState(895);
				match(NUMBER);
				}
				break;
			case MINUS:
				enterOuterAlt(_localctx, 2);
				{
				setState(896);
				match(MINUS);
				setState(897);
				match(NUMBER);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Signed_real_numberContext extends ParserRuleContext {
		public Real_numberContext real_number() {
			return getRuleContext(Real_numberContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(Python3Parser.MINUS, 0); }
		public Signed_real_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signed_real_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSigned_real_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSigned_real_number(this);
		}
	}

	public final Signed_real_numberContext signed_real_number() throws RecognitionException {
		Signed_real_numberContext _localctx = new Signed_real_numberContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_signed_real_number);
		try {
			setState(903);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NUMBER:
				enterOuterAlt(_localctx, 1);
				{
				setState(900);
				real_number();
				}
				break;
			case MINUS:
				enterOuterAlt(_localctx, 2);
				{
				setState(901);
				match(MINUS);
				setState(902);
				real_number();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Real_numberContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(Python3Parser.NUMBER, 0); }
		public Real_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_real_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterReal_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitReal_number(this);
		}
	}

	public final Real_numberContext real_number() throws RecognitionException {
		Real_numberContext _localctx = new Real_numberContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_real_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(905);
			match(NUMBER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Imaginary_numberContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(Python3Parser.NUMBER, 0); }
		public Imaginary_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_imaginary_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterImaginary_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitImaginary_number(this);
		}
	}

	public final Imaginary_numberContext imaginary_number() throws RecognitionException {
		Imaginary_numberContext _localctx = new Imaginary_numberContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_imaginary_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(907);
			match(NUMBER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Capture_patternContext extends ParserRuleContext {
		public Pattern_capture_targetContext pattern_capture_target() {
			return getRuleContext(Pattern_capture_targetContext.class,0);
		}
		public Capture_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_capture_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterCapture_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitCapture_pattern(this);
		}
	}

	public final Capture_patternContext capture_pattern() throws RecognitionException {
		Capture_patternContext _localctx = new Capture_patternContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_capture_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(909);
			pattern_capture_target();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Pattern_capture_targetContext extends ParserRuleContext {
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Pattern_capture_targetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pattern_capture_target; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterPattern_capture_target(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitPattern_capture_target(this);
		}
	}

	public final Pattern_capture_targetContext pattern_capture_target() throws RecognitionException {
		Pattern_capture_targetContext _localctx = new Pattern_capture_targetContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_pattern_capture_target);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(911);
			name();
			setState(912);
			if (!( this.CannotBeDotLpEq() )) throw new FailedPredicateException(this, " this.CannotBeDotLpEq() ");
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Wildcard_patternContext extends ParserRuleContext {
		public TerminalNode UNDERSCORE() { return getToken(Python3Parser.UNDERSCORE, 0); }
		public Wildcard_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_wildcard_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterWildcard_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitWildcard_pattern(this);
		}
	}

	public final Wildcard_patternContext wildcard_pattern() throws RecognitionException {
		Wildcard_patternContext _localctx = new Wildcard_patternContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_wildcard_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(914);
			match(UNDERSCORE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Value_patternContext extends ParserRuleContext {
		public AttrContext attr() {
			return getRuleContext(AttrContext.class,0);
		}
		public Value_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterValue_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitValue_pattern(this);
		}
	}

	public final Value_patternContext value_pattern() throws RecognitionException {
		Value_patternContext _localctx = new Value_patternContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_value_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(916);
			attr();
			setState(917);
			if (!( this.CannotBeDotLpEq() )) throw new FailedPredicateException(this, " this.CannotBeDotLpEq() ");
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AttrContext extends ParserRuleContext {
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(Python3Parser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(Python3Parser.DOT, i);
		}
		public AttrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAttr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAttr(this);
		}
	}

	public final AttrContext attr() throws RecognitionException {
		AttrContext _localctx = new AttrContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_attr);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(919);
			name();
			setState(922);
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(920);
					match(DOT);
					setState(921);
					name();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(924);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,114,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Name_or_attrContext extends ParserRuleContext {
		public AttrContext attr() {
			return getRuleContext(AttrContext.class,0);
		}
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Name_or_attrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name_or_attr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterName_or_attr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitName_or_attr(this);
		}
	}

	public final Name_or_attrContext name_or_attr() throws RecognitionException {
		Name_or_attrContext _localctx = new Name_or_attrContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_name_or_attr);
		try {
			setState(928);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,115,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(926);
				attr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(927);
				name();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Group_patternContext extends ParserRuleContext {
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public PatternContext pattern() {
			return getRuleContext(PatternContext.class,0);
		}
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public Group_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_group_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterGroup_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitGroup_pattern(this);
		}
	}

	public final Group_patternContext group_pattern() throws RecognitionException {
		Group_patternContext _localctx = new Group_patternContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_group_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(930);
			match(OPEN_PAREN);
			setState(931);
			pattern();
			setState(932);
			match(CLOSE_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sequence_patternContext extends ParserRuleContext {
		public TerminalNode OPEN_BRACK() { return getToken(Python3Parser.OPEN_BRACK, 0); }
		public TerminalNode CLOSE_BRACK() { return getToken(Python3Parser.CLOSE_BRACK, 0); }
		public Maybe_sequence_patternContext maybe_sequence_pattern() {
			return getRuleContext(Maybe_sequence_patternContext.class,0);
		}
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public Open_sequence_patternContext open_sequence_pattern() {
			return getRuleContext(Open_sequence_patternContext.class,0);
		}
		public Sequence_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sequence_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSequence_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSequence_pattern(this);
		}
	}

	public final Sequence_patternContext sequence_pattern() throws RecognitionException {
		Sequence_patternContext _localctx = new Sequence_patternContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_sequence_pattern);
		int _la;
		try {
			setState(944);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPEN_BRACK:
				enterOuterAlt(_localctx, 1);
				{
				setState(934);
				match(OPEN_BRACK);
				setState(936);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 216209344097681432L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 8449L) != 0)) {
					{
					setState(935);
					maybe_sequence_pattern();
					}
				}

				setState(938);
				match(CLOSE_BRACK);
				}
				break;
			case OPEN_PAREN:
				enterOuterAlt(_localctx, 2);
				{
				setState(939);
				match(OPEN_PAREN);
				setState(941);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 216209344097681432L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 8449L) != 0)) {
					{
					setState(940);
					open_sequence_pattern();
					}
				}

				setState(943);
				match(CLOSE_PAREN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Open_sequence_patternContext extends ParserRuleContext {
		public Maybe_star_patternContext maybe_star_pattern() {
			return getRuleContext(Maybe_star_patternContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(Python3Parser.COMMA, 0); }
		public Maybe_sequence_patternContext maybe_sequence_pattern() {
			return getRuleContext(Maybe_sequence_patternContext.class,0);
		}
		public Open_sequence_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_open_sequence_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterOpen_sequence_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitOpen_sequence_pattern(this);
		}
	}

	public final Open_sequence_patternContext open_sequence_pattern() throws RecognitionException {
		Open_sequence_patternContext _localctx = new Open_sequence_patternContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_open_sequence_pattern);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(946);
			maybe_star_pattern();
			setState(947);
			match(COMMA);
			setState(949);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 216209344097681432L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 8449L) != 0)) {
				{
				setState(948);
				maybe_sequence_pattern();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Maybe_sequence_patternContext extends ParserRuleContext {
		public List<Maybe_star_patternContext> maybe_star_pattern() {
			return getRuleContexts(Maybe_star_patternContext.class);
		}
		public Maybe_star_patternContext maybe_star_pattern(int i) {
			return getRuleContext(Maybe_star_patternContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Maybe_sequence_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_maybe_sequence_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterMaybe_sequence_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitMaybe_sequence_pattern(this);
		}
	}

	public final Maybe_sequence_patternContext maybe_sequence_pattern() throws RecognitionException {
		Maybe_sequence_patternContext _localctx = new Maybe_sequence_patternContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_maybe_sequence_pattern);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(951);
			maybe_star_pattern();
			setState(956);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,120,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(952);
					match(COMMA);
					setState(953);
					maybe_star_pattern();
					}
					}
				}
				setState(958);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,120,_ctx);
			}
			setState(960);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(959);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Maybe_star_patternContext extends ParserRuleContext {
		public Star_patternContext star_pattern() {
			return getRuleContext(Star_patternContext.class,0);
		}
		public PatternContext pattern() {
			return getRuleContext(PatternContext.class,0);
		}
		public Maybe_star_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_maybe_star_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterMaybe_star_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitMaybe_star_pattern(this);
		}
	}

	public final Maybe_star_patternContext maybe_star_pattern() throws RecognitionException {
		Maybe_star_patternContext _localctx = new Maybe_star_patternContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_maybe_star_pattern);
		try {
			setState(964);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(962);
				star_pattern();
				}
				break;
			case STRING:
			case NUMBER:
			case FALSE:
			case MATCH:
			case NONE:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case MINUS:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 2);
				{
				setState(963);
				pattern();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Star_patternContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public Pattern_capture_targetContext pattern_capture_target() {
			return getRuleContext(Pattern_capture_targetContext.class,0);
		}
		public Wildcard_patternContext wildcard_pattern() {
			return getRuleContext(Wildcard_patternContext.class,0);
		}
		public Star_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_star_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterStar_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitStar_pattern(this);
		}
	}

	public final Star_patternContext star_pattern() throws RecognitionException {
		Star_patternContext _localctx = new Star_patternContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_star_pattern);
		try {
			setState(970);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,123,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(966);
				match(STAR);
				setState(967);
				pattern_capture_target();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(968);
				match(STAR);
				setState(969);
				wildcard_pattern();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Mapping_patternContext extends ParserRuleContext {
		public TerminalNode OPEN_BRACE() { return getToken(Python3Parser.OPEN_BRACE, 0); }
		public TerminalNode CLOSE_BRACE() { return getToken(Python3Parser.CLOSE_BRACE, 0); }
		public Double_star_patternContext double_star_pattern() {
			return getRuleContext(Double_star_patternContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Items_patternContext items_pattern() {
			return getRuleContext(Items_patternContext.class,0);
		}
		public Mapping_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mapping_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterMapping_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitMapping_pattern(this);
		}
	}

	public final Mapping_patternContext mapping_pattern() throws RecognitionException {
		Mapping_patternContext _localctx = new Mapping_patternContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_mapping_pattern);
		int _la;
		try {
			setState(997);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(972);
				match(OPEN_BRACE);
				setState(973);
				match(CLOSE_BRACE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(974);
				match(OPEN_BRACE);
				setState(975);
				double_star_pattern();
				setState(977);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(976);
					match(COMMA);
					}
				}

				setState(979);
				match(CLOSE_BRACE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(981);
				match(OPEN_BRACE);
				setState(982);
				items_pattern();
				setState(983);
				match(COMMA);
				setState(984);
				double_star_pattern();
				setState(986);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(985);
					match(COMMA);
					}
				}

				setState(988);
				match(CLOSE_BRACE);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(990);
				match(OPEN_BRACE);
				setState(991);
				items_pattern();
				setState(993);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(992);
					match(COMMA);
					}
				}

				setState(995);
				match(CLOSE_BRACE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Items_patternContext extends ParserRuleContext {
		public List<Key_value_patternContext> key_value_pattern() {
			return getRuleContexts(Key_value_patternContext.class);
		}
		public Key_value_patternContext key_value_pattern(int i) {
			return getRuleContext(Key_value_patternContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Items_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_items_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterItems_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitItems_pattern(this);
		}
	}

	public final Items_patternContext items_pattern() throws RecognitionException {
		Items_patternContext _localctx = new Items_patternContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_items_pattern);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(999);
			key_value_pattern();
			setState(1004);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,128,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1000);
					match(COMMA);
					setState(1001);
					key_value_pattern();
					}
					}
				}
				setState(1006);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,128,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Key_value_patternContext extends ParserRuleContext {
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public PatternContext pattern() {
			return getRuleContext(PatternContext.class,0);
		}
		public Literal_exprContext literal_expr() {
			return getRuleContext(Literal_exprContext.class,0);
		}
		public AttrContext attr() {
			return getRuleContext(AttrContext.class,0);
		}
		public Key_value_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_key_value_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterKey_value_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitKey_value_pattern(this);
		}
	}

	public final Key_value_patternContext key_value_pattern() throws RecognitionException {
		Key_value_patternContext _localctx = new Key_value_patternContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_key_value_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1009);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case FALSE:
			case NONE:
			case TRUE:
			case MINUS:
				{
				setState(1007);
				literal_expr();
				}
				break;
			case MATCH:
			case UNDERSCORE:
			case NAME:
				{
				setState(1008);
				attr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1011);
			match(COLON);
			setState(1012);
			pattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Double_star_patternContext extends ParserRuleContext {
		public TerminalNode POWER() { return getToken(Python3Parser.POWER, 0); }
		public Pattern_capture_targetContext pattern_capture_target() {
			return getRuleContext(Pattern_capture_targetContext.class,0);
		}
		public Double_star_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_double_star_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDouble_star_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDouble_star_pattern(this);
		}
	}

	public final Double_star_patternContext double_star_pattern() throws RecognitionException {
		Double_star_patternContext _localctx = new Double_star_patternContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_double_star_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1014);
			match(POWER);
			setState(1015);
			pattern_capture_target();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Class_patternContext extends ParserRuleContext {
		public Name_or_attrContext name_or_attr() {
			return getRuleContext(Name_or_attrContext.class,0);
		}
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public Positional_patternsContext positional_patterns() {
			return getRuleContext(Positional_patternsContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Keyword_patternsContext keyword_patterns() {
			return getRuleContext(Keyword_patternsContext.class,0);
		}
		public Class_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_class_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterClass_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitClass_pattern(this);
		}
	}

	public final Class_patternContext class_pattern() throws RecognitionException {
		Class_patternContext _localctx = new Class_patternContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_class_pattern);
		int _la;
		try {
			setState(1047);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,133,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1017);
				name_or_attr();
				setState(1018);
				match(OPEN_PAREN);
				setState(1019);
				match(CLOSE_PAREN);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1021);
				name_or_attr();
				setState(1022);
				match(OPEN_PAREN);
				setState(1023);
				positional_patterns();
				setState(1025);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1024);
					match(COMMA);
					}
				}

				setState(1027);
				match(CLOSE_PAREN);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1029);
				name_or_attr();
				setState(1030);
				match(OPEN_PAREN);
				setState(1031);
				keyword_patterns();
				setState(1033);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1032);
					match(COMMA);
					}
				}

				setState(1035);
				match(CLOSE_PAREN);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1037);
				name_or_attr();
				setState(1038);
				match(OPEN_PAREN);
				setState(1039);
				positional_patterns();
				setState(1040);
				match(COMMA);
				setState(1041);
				keyword_patterns();
				setState(1043);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1042);
					match(COMMA);
					}
				}

				setState(1045);
				match(CLOSE_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Positional_patternsContext extends ParserRuleContext {
		public List<PatternContext> pattern() {
			return getRuleContexts(PatternContext.class);
		}
		public PatternContext pattern(int i) {
			return getRuleContext(PatternContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Positional_patternsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_positional_patterns; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterPositional_patterns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitPositional_patterns(this);
		}
	}

	public final Positional_patternsContext positional_patterns() throws RecognitionException {
		Positional_patternsContext _localctx = new Positional_patternsContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_positional_patterns);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1049);
			pattern();
			setState(1054);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,134,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1050);
					match(COMMA);
					setState(1051);
					pattern();
					}
					}
				}
				setState(1056);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,134,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Keyword_patternsContext extends ParserRuleContext {
		public List<Keyword_patternContext> keyword_pattern() {
			return getRuleContexts(Keyword_patternContext.class);
		}
		public Keyword_patternContext keyword_pattern(int i) {
			return getRuleContext(Keyword_patternContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Keyword_patternsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyword_patterns; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterKeyword_patterns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitKeyword_patterns(this);
		}
	}

	public final Keyword_patternsContext keyword_patterns() throws RecognitionException {
		Keyword_patternsContext _localctx = new Keyword_patternsContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_keyword_patterns);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1057);
			keyword_pattern();
			setState(1062);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,135,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1058);
					match(COMMA);
					setState(1059);
					keyword_pattern();
					}
					}
				}
				setState(1064);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,135,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Keyword_patternContext extends ParserRuleContext {
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode ASSIGN() { return getToken(Python3Parser.ASSIGN, 0); }
		public PatternContext pattern() {
			return getRuleContext(PatternContext.class,0);
		}
		public Keyword_patternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyword_pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterKeyword_pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitKeyword_pattern(this);
		}
	}

	public final Keyword_patternContext keyword_pattern() throws RecognitionException {
		Keyword_patternContext _localctx = new Keyword_patternContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_keyword_pattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1065);
			name();
			setState(1066);
			match(ASSIGN);
			setState(1067);
			pattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TestContext extends ParserRuleContext {
		public List<Or_testContext> or_test() {
			return getRuleContexts(Or_testContext.class);
		}
		public Or_testContext or_test(int i) {
			return getRuleContext(Or_testContext.class,i);
		}
		public TerminalNode IF() { return getToken(Python3Parser.IF, 0); }
		public TerminalNode ELSE() { return getToken(Python3Parser.ELSE, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public LambdefContext lambdef() {
			return getRuleContext(LambdefContext.class,0);
		}
		public TestContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_test; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTest(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTest(this);
		}
	}

	public final TestContext test() throws RecognitionException {
		TestContext _localctx = new TestContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_test);
		int _la;
		try {
			setState(1078);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1069);
				or_test();
				setState(1075);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(1070);
					match(IF);
					setState(1071);
					or_test();
					setState(1072);
					match(ELSE);
					setState(1073);
					test();
					}
				}

				}
				break;
			case LAMBDA:
				enterOuterAlt(_localctx, 2);
				{
				setState(1077);
				lambdef();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Test_nocondContext extends ParserRuleContext {
		public Or_testContext or_test() {
			return getRuleContext(Or_testContext.class,0);
		}
		public Lambdef_nocondContext lambdef_nocond() {
			return getRuleContext(Lambdef_nocondContext.class,0);
		}
		public Test_nocondContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_test_nocond; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTest_nocond(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTest_nocond(this);
		}
	}

	public final Test_nocondContext test_nocond() throws RecognitionException {
		Test_nocondContext _localctx = new Test_nocondContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_test_nocond);
		try {
			setState(1082);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1080);
				or_test();
				}
				break;
			case LAMBDA:
				enterOuterAlt(_localctx, 2);
				{
				setState(1081);
				lambdef_nocond();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LambdefContext extends ParserRuleContext {
		public TerminalNode LAMBDA() { return getToken(Python3Parser.LAMBDA, 0); }
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public VarargslistContext varargslist() {
			return getRuleContext(VarargslistContext.class,0);
		}
		public LambdefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lambdef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterLambdef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitLambdef(this);
		}
	}

	public final LambdefContext lambdef() throws RecognitionException {
		LambdefContext _localctx = new LambdefContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_lambdef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1084);
			match(LAMBDA);
			setState(1086);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4683779897422774272L) != 0)) {
				{
				setState(1085);
				varargslist();
				}
			}

			setState(1088);
			match(COLON);
			setState(1089);
			test();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Lambdef_nocondContext extends ParserRuleContext {
		public TerminalNode LAMBDA() { return getToken(Python3Parser.LAMBDA, 0); }
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public Test_nocondContext test_nocond() {
			return getRuleContext(Test_nocondContext.class,0);
		}
		public VarargslistContext varargslist() {
			return getRuleContext(VarargslistContext.class,0);
		}
		public Lambdef_nocondContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lambdef_nocond; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterLambdef_nocond(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitLambdef_nocond(this);
		}
	}

	public final Lambdef_nocondContext lambdef_nocond() throws RecognitionException {
		Lambdef_nocondContext _localctx = new Lambdef_nocondContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_lambdef_nocond);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1091);
			match(LAMBDA);
			setState(1093);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4683779897422774272L) != 0)) {
				{
				setState(1092);
				varargslist();
				}
			}

			setState(1095);
			match(COLON);
			setState(1096);
			test_nocond();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Or_testContext extends ParserRuleContext {
		public List<And_testContext> and_test() {
			return getRuleContexts(And_testContext.class);
		}
		public And_testContext and_test(int i) {
			return getRuleContext(And_testContext.class,i);
		}
		public List<TerminalNode> OR() { return getTokens(Python3Parser.OR); }
		public TerminalNode OR(int i) {
			return getToken(Python3Parser.OR, i);
		}
		public Or_testContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_or_test; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterOr_test(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitOr_test(this);
		}
	}

	public final Or_testContext or_test() throws RecognitionException {
		Or_testContext _localctx = new Or_testContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_or_test);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1098);
			and_test();
			setState(1103);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OR) {
				{
				{
				setState(1099);
				match(OR);
				setState(1100);
				and_test();
				}
				}
				setState(1105);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class And_testContext extends ParserRuleContext {
		public List<Not_testContext> not_test() {
			return getRuleContexts(Not_testContext.class);
		}
		public Not_testContext not_test(int i) {
			return getRuleContext(Not_testContext.class,i);
		}
		public List<TerminalNode> AND() { return getTokens(Python3Parser.AND); }
		public TerminalNode AND(int i) {
			return getToken(Python3Parser.AND, i);
		}
		public And_testContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_and_test; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAnd_test(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAnd_test(this);
		}
	}

	public final And_testContext and_test() throws RecognitionException {
		And_testContext _localctx = new And_testContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_and_test);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1106);
			not_test();
			setState(1111);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==AND) {
				{
				{
				setState(1107);
				match(AND);
				setState(1108);
				not_test();
				}
				}
				setState(1113);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Not_testContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(Python3Parser.NOT, 0); }
		public Not_testContext not_test() {
			return getRuleContext(Not_testContext.class,0);
		}
		public ComparisonContext comparison() {
			return getRuleContext(ComparisonContext.class,0);
		}
		public Not_testContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not_test; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterNot_test(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitNot_test(this);
		}
	}

	public final Not_testContext not_test() throws RecognitionException {
		Not_testContext _localctx = new Not_testContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_not_test);
		try {
			setState(1117);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NOT:
				enterOuterAlt(_localctx, 1);
				{
				setState(1114);
				match(NOT);
				setState(1115);
				not_test();
				}
				break;
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case MATCH:
			case NONE:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1116);
				comparison();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends ParserRuleContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Comp_opContext> comp_op() {
			return getRuleContexts(Comp_opContext.class);
		}
		public Comp_opContext comp_op(int i) {
			return getRuleContext(Comp_opContext.class,i);
		}
		public ComparisonContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparison; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitComparison(this);
		}
	}

	public final ComparisonContext comparison() throws RecognitionException {
		ComparisonContext _localctx = new ComparisonContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_comparison);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1119);
			expr(0);
			setState(1125);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1120);
					comp_op();
					setState(1121);
					expr(0);
					}
					}
				}
				setState(1127);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Comp_opContext extends ParserRuleContext {
		public TerminalNode LESS_THAN() { return getToken(Python3Parser.LESS_THAN, 0); }
		public TerminalNode GREATER_THAN() { return getToken(Python3Parser.GREATER_THAN, 0); }
		public TerminalNode EQUALS() { return getToken(Python3Parser.EQUALS, 0); }
		public TerminalNode GT_EQ() { return getToken(Python3Parser.GT_EQ, 0); }
		public TerminalNode LT_EQ() { return getToken(Python3Parser.LT_EQ, 0); }
		public TerminalNode NOT_EQ_1() { return getToken(Python3Parser.NOT_EQ_1, 0); }
		public TerminalNode NOT_EQ_2() { return getToken(Python3Parser.NOT_EQ_2, 0); }
		public TerminalNode IN() { return getToken(Python3Parser.IN, 0); }
		public TerminalNode NOT() { return getToken(Python3Parser.NOT, 0); }
		public TerminalNode IS() { return getToken(Python3Parser.IS, 0); }
		public Comp_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_op; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterComp_op(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitComp_op(this);
		}
	}

	public final Comp_opContext comp_op() throws RecognitionException {
		Comp_opContext _localctx = new Comp_opContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_comp_op);
		try {
			setState(1141);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1128);
				match(LESS_THAN);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1129);
				match(GREATER_THAN);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1130);
				match(EQUALS);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1131);
				match(GT_EQ);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1132);
				match(LT_EQ);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1133);
				match(NOT_EQ_1);
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1134);
				match(NOT_EQ_2);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(1135);
				match(IN);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(1136);
				match(NOT);
				setState(1137);
				match(IN);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(1138);
				match(IS);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(1139);
				match(IS);
				setState(1140);
				match(NOT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Star_exprContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Star_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_star_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterStar_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitStar_expr(this);
		}
	}

	public final Star_exprContext star_expr() throws RecognitionException {
		Star_exprContext _localctx = new Star_exprContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_star_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1143);
			match(STAR);
			setState(1144);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExprContext extends ParserRuleContext {
		public Atom_exprContext atom_expr() {
			return getRuleContext(Atom_exprContext.class,0);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> ADD() { return getTokens(Python3Parser.ADD); }
		public TerminalNode ADD(int i) {
			return getToken(Python3Parser.ADD, i);
		}
		public List<TerminalNode> MINUS() { return getTokens(Python3Parser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(Python3Parser.MINUS, i);
		}
		public List<TerminalNode> NOT_OP() { return getTokens(Python3Parser.NOT_OP); }
		public TerminalNode NOT_OP(int i) {
			return getToken(Python3Parser.NOT_OP, i);
		}
		public TerminalNode POWER() { return getToken(Python3Parser.POWER, 0); }
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public TerminalNode AT() { return getToken(Python3Parser.AT, 0); }
		public TerminalNode DIV() { return getToken(Python3Parser.DIV, 0); }
		public TerminalNode MOD() { return getToken(Python3Parser.MOD, 0); }
		public TerminalNode IDIV() { return getToken(Python3Parser.IDIV, 0); }
		public TerminalNode LEFT_SHIFT() { return getToken(Python3Parser.LEFT_SHIFT, 0); }
		public TerminalNode RIGHT_SHIFT() { return getToken(Python3Parser.RIGHT_SHIFT, 0); }
		public TerminalNode AND_OP() { return getToken(Python3Parser.AND_OP, 0); }
		public TerminalNode XOR() { return getToken(Python3Parser.XOR, 0); }
		public TerminalNode OR_OP() { return getToken(Python3Parser.OR_OP, 0); }
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitExpr(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 194;
		enterRecursionRule(_localctx, 194, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1154);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case MATCH:
			case NONE:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case OPEN_BRACE:
				{
				setState(1147);
				atom_expr();
				}
				break;
			case ADD:
			case MINUS:
			case NOT_OP:
				{
				setState(1149);
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(1148);
						_la = _input.LA(1);
						if ( !(((((_la - 71)) & ~0x3f) == 0 && ((1L << (_la - 71)) & 35L) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(1151);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,146,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				setState(1153);
				expr(7);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(1179);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,149,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1177);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,148,_ctx) ) {
					case 1:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1156);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(1157);
						match(POWER);
						setState(1158);
						expr(9);
						}
						break;
					case 2:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1159);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(1160);
						_la = _input.LA(1);
						if ( !(((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & 1074659329L) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1161);
						expr(7);
						}
						break;
					case 3:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1162);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(1163);
						_la = _input.LA(1);
						if ( !(_la==ADD || _la==MINUS) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1164);
						expr(6);
						}
						break;
					case 4:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1165);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(1166);
						_la = _input.LA(1);
						if ( !(_la==LEFT_SHIFT || _la==RIGHT_SHIFT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1167);
						expr(5);
						}
						break;
					case 5:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1168);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1169);
						match(AND_OP);
						setState(1170);
						expr(4);
						}
						break;
					case 6:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1171);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1172);
						match(XOR);
						setState(1173);
						expr(3);
						}
						break;
					case 7:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1174);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1175);
						match(OR_OP);
						setState(1176);
						expr(2);
						}
						break;
					}
					}
				}
				setState(1181);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,149,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Atom_exprContext extends ParserRuleContext {
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public TerminalNode AWAIT() { return getToken(Python3Parser.AWAIT, 0); }
		public List<TrailerContext> trailer() {
			return getRuleContexts(TrailerContext.class);
		}
		public TrailerContext trailer(int i) {
			return getRuleContext(TrailerContext.class,i);
		}
		public Atom_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_atom_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAtom_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAtom_expr(this);
		}
	}

	public final Atom_exprContext atom_expr() throws RecognitionException {
		Atom_exprContext _localctx = new Atom_exprContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_atom_expr);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1183);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AWAIT) {
				{
				setState(1182);
				match(AWAIT);
				}
			}

			setState(1185);
			atom();
			setState(1189);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,151,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1186);
					trailer();
					}
					}
				}
				setState(1191);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,151,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AtomContext extends ParserRuleContext {
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public Yield_exprContext yield_expr() {
			return getRuleContext(Yield_exprContext.class,0);
		}
		public Testlist_compContext testlist_comp() {
			return getRuleContext(Testlist_compContext.class,0);
		}
		public TerminalNode OPEN_BRACK() { return getToken(Python3Parser.OPEN_BRACK, 0); }
		public TerminalNode CLOSE_BRACK() { return getToken(Python3Parser.CLOSE_BRACK, 0); }
		public TerminalNode OPEN_BRACE() { return getToken(Python3Parser.OPEN_BRACE, 0); }
		public TerminalNode CLOSE_BRACE() { return getToken(Python3Parser.CLOSE_BRACE, 0); }
		public DictorsetmakerContext dictorsetmaker() {
			return getRuleContext(DictorsetmakerContext.class,0);
		}
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode NUMBER() { return getToken(Python3Parser.NUMBER, 0); }
		public List<TerminalNode> STRING() { return getTokens(Python3Parser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(Python3Parser.STRING, i);
		}
		public TerminalNode ELLIPSIS() { return getToken(Python3Parser.ELLIPSIS, 0); }
		public TerminalNode NONE() { return getToken(Python3Parser.NONE, 0); }
		public TerminalNode TRUE() { return getToken(Python3Parser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(Python3Parser.FALSE, 0); }
		public AtomContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_atom; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitAtom(this);
		}
	}

	public final AtomContext atom() throws RecognitionException {
		AtomContext _localctx = new AtomContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_atom);
		int _la;
		try {
			int _alt;
			setState(1219);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPEN_PAREN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1192);
				match(OPEN_PAREN);
				setState(1195);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case YIELD:
					{
					setState(1193);
					yield_expr();
					}
					break;
				case STRING:
				case NUMBER:
				case AWAIT:
				case FALSE:
				case LAMBDA:
				case MATCH:
				case NONE:
				case NOT:
				case TRUE:
				case UNDERSCORE:
				case NAME:
				case ELLIPSIS:
				case STAR:
				case OPEN_PAREN:
				case OPEN_BRACK:
				case ADD:
				case MINUS:
				case NOT_OP:
				case OPEN_BRACE:
					{
					setState(1194);
					testlist_comp();
					}
					break;
				case CLOSE_PAREN:
					break;
				default:
					break;
				}
				setState(1197);
				match(CLOSE_PAREN);
				}
				break;
			case OPEN_BRACK:
				enterOuterAlt(_localctx, 2);
				{
				setState(1198);
				match(OPEN_BRACK);
				setState(1200);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 252238150243451928L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(1199);
					testlist_comp();
					}
				}

				setState(1202);
				match(CLOSE_BRACK);
				}
				break;
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1203);
				match(OPEN_BRACE);
				setState(1205);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4863924168670839832L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(1204);
					dictorsetmaker();
					}
				}

				setState(1207);
				match(CLOSE_BRACE);
				}
				break;
			case MATCH:
			case UNDERSCORE:
			case NAME:
				enterOuterAlt(_localctx, 4);
				{
				setState(1208);
				name();
				}
				break;
			case NUMBER:
				enterOuterAlt(_localctx, 5);
				{
				setState(1209);
				match(NUMBER);
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 6);
				{
				setState(1211);
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(1210);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(1213);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,155,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			case ELLIPSIS:
				enterOuterAlt(_localctx, 7);
				{
				setState(1215);
				match(ELLIPSIS);
				}
				break;
			case NONE:
				enterOuterAlt(_localctx, 8);
				{
				setState(1216);
				match(NONE);
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 9);
				{
				setState(1217);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 10);
				{
				setState(1218);
				match(FALSE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NameContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(Python3Parser.NAME, 0); }
		public TerminalNode UNDERSCORE() { return getToken(Python3Parser.UNDERSCORE, 0); }
		public TerminalNode MATCH() { return getToken(Python3Parser.MATCH, 0); }
		public NameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitName(this);
		}
	}

	public final NameContext name() throws RecognitionException {
		NameContext _localctx = new NameContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1221);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 36284957458432L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Testlist_compContext extends ParserRuleContext {
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<Star_exprContext> star_expr() {
			return getRuleContexts(Star_exprContext.class);
		}
		public Star_exprContext star_expr(int i) {
			return getRuleContext(Star_exprContext.class,i);
		}
		public Comp_forContext comp_for() {
			return getRuleContext(Comp_forContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public Testlist_compContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_testlist_comp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTestlist_comp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTestlist_comp(this);
		}
	}

	public final Testlist_compContext testlist_comp() throws RecognitionException {
		Testlist_compContext _localctx = new Testlist_compContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_testlist_comp);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1225);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case LAMBDA:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				{
				setState(1223);
				test();
				}
				break;
			case STAR:
				{
				setState(1224);
				star_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1241);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ASYNC:
			case FOR:
				{
				setState(1227);
				comp_for();
				}
				break;
			case CLOSE_PAREN:
			case COMMA:
			case CLOSE_BRACK:
				{
				setState(1235);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,159,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1228);
						match(COMMA);
						setState(1231);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
						case STRING:
						case NUMBER:
						case AWAIT:
						case FALSE:
						case LAMBDA:
						case MATCH:
						case NONE:
						case NOT:
						case TRUE:
						case UNDERSCORE:
						case NAME:
						case ELLIPSIS:
						case OPEN_PAREN:
						case OPEN_BRACK:
						case ADD:
						case MINUS:
						case NOT_OP:
						case OPEN_BRACE:
							{
							setState(1229);
							test();
							}
							break;
						case STAR:
							{
							setState(1230);
							star_expr();
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						}
						}
					}
					setState(1237);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,159,_ctx);
				}
				setState(1239);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1238);
					match(COMMA);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TrailerContext extends ParserRuleContext {
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public ArglistContext arglist() {
			return getRuleContext(ArglistContext.class,0);
		}
		public TerminalNode OPEN_BRACK() { return getToken(Python3Parser.OPEN_BRACK, 0); }
		public SubscriptlistContext subscriptlist() {
			return getRuleContext(SubscriptlistContext.class,0);
		}
		public TerminalNode CLOSE_BRACK() { return getToken(Python3Parser.CLOSE_BRACK, 0); }
		public TerminalNode DOT() { return getToken(Python3Parser.DOT, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TrailerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trailer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTrailer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTrailer(this);
		}
	}

	public final TrailerContext trailer() throws RecognitionException {
		TrailerContext _localctx = new TrailerContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_trailer);
		int _la;
		try {
			setState(1254);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPEN_PAREN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1243);
				match(OPEN_PAREN);
				setState(1245);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4863924168670839832L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(1244);
					arglist();
					}
				}

				setState(1247);
				match(CLOSE_PAREN);
				}
				break;
			case OPEN_BRACK:
				enterOuterAlt(_localctx, 2);
				{
				setState(1248);
				match(OPEN_BRACK);
				setState(1249);
				subscriptlist();
				setState(1250);
				match(CLOSE_BRACK);
				}
				break;
			case DOT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1252);
				match(DOT);
				setState(1253);
				name();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptlistContext extends ParserRuleContext {
		public List<Subscript_Context> subscript_() {
			return getRuleContexts(Subscript_Context.class);
		}
		public Subscript_Context subscript_(int i) {
			return getRuleContext(Subscript_Context.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public SubscriptlistContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subscriptlist; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSubscriptlist(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSubscriptlist(this);
		}
	}

	public final SubscriptlistContext subscriptlist() throws RecognitionException {
		SubscriptlistContext _localctx = new SubscriptlistContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_subscriptlist);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1256);
			subscript_();
			setState(1261);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,164,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1257);
					match(COMMA);
					setState(1258);
					subscript_();
					}
					}
				}
				setState(1263);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,164,_ctx);
			}
			setState(1265);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(1264);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Subscript_Context extends ParserRuleContext {
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public SliceopContext sliceop() {
			return getRuleContext(SliceopContext.class,0);
		}
		public Subscript_Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subscript_; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSubscript_(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSubscript_(this);
		}
	}

	public final Subscript_Context subscript_() throws RecognitionException {
		Subscript_Context _localctx = new Subscript_Context(_ctx, getState());
		enterRule(_localctx, 208, RULE_subscript_);
		int _la;
		try {
			setState(1278);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,169,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1267);
				test();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1269);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556205523992L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(1268);
					test();
					}
				}

				setState(1271);
				match(COLON);
				setState(1273);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556205523992L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(1272);
					test();
					}
				}

				setState(1276);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COLON) {
					{
					setState(1275);
					sliceop();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SliceopContext extends ParserRuleContext {
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public SliceopContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sliceop; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterSliceop(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitSliceop(this);
		}
	}

	public final SliceopContext sliceop() throws RecognitionException {
		SliceopContext _localctx = new SliceopContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_sliceop);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1280);
			match(COLON);
			setState(1282);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556205523992L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
				{
				setState(1281);
				test();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExprlistContext extends ParserRuleContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Star_exprContext> star_expr() {
			return getRuleContexts(Star_exprContext.class);
		}
		public Star_exprContext star_expr(int i) {
			return getRuleContext(Star_exprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public ExprlistContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exprlist; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterExprlist(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitExprlist(this);
		}
	}

	public final ExprlistContext exprlist() throws RecognitionException {
		ExprlistContext _localctx = new ExprlistContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_exprlist);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1286);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case MATCH:
			case NONE:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				{
				setState(1284);
				expr(0);
				}
				break;
			case STAR:
				{
				setState(1285);
				star_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1295);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,173,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1288);
					match(COMMA);
					setState(1291);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case STRING:
					case NUMBER:
					case AWAIT:
					case FALSE:
					case MATCH:
					case NONE:
					case TRUE:
					case UNDERSCORE:
					case NAME:
					case ELLIPSIS:
					case OPEN_PAREN:
					case OPEN_BRACK:
					case ADD:
					case MINUS:
					case NOT_OP:
					case OPEN_BRACE:
						{
						setState(1289);
						expr(0);
						}
						break;
					case STAR:
						{
						setState(1290);
						star_expr();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
				}
				setState(1297);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,173,_ctx);
			}
			setState(1299);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(1298);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TestlistContext extends ParserRuleContext {
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public TestlistContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_testlist; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterTestlist(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitTestlist(this);
		}
	}

	public final TestlistContext testlist() throws RecognitionException {
		TestlistContext _localctx = new TestlistContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_testlist);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1301);
			test();
			setState(1306);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,175,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1302);
					match(COMMA);
					setState(1303);
					test();
					}
					}
				}
				setState(1308);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,175,_ctx);
			}
			setState(1310);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(1309);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DictorsetmakerContext extends ParserRuleContext {
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public List<TerminalNode> COLON() { return getTokens(Python3Parser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(Python3Parser.COLON, i);
		}
		public List<TerminalNode> POWER() { return getTokens(Python3Parser.POWER); }
		public TerminalNode POWER(int i) {
			return getToken(Python3Parser.POWER, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public Comp_forContext comp_for() {
			return getRuleContext(Comp_forContext.class,0);
		}
		public List<Star_exprContext> star_expr() {
			return getRuleContexts(Star_exprContext.class);
		}
		public Star_exprContext star_expr(int i) {
			return getRuleContext(Star_exprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public DictorsetmakerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dictorsetmaker; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterDictorsetmaker(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitDictorsetmaker(this);
		}
	}

	public final DictorsetmakerContext dictorsetmaker() throws RecognitionException {
		DictorsetmakerContext _localctx = new DictorsetmakerContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_dictorsetmaker);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1360);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
			case 1:
				{
				{
				setState(1318);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case NUMBER:
				case AWAIT:
				case FALSE:
				case LAMBDA:
				case MATCH:
				case NONE:
				case NOT:
				case TRUE:
				case UNDERSCORE:
				case NAME:
				case ELLIPSIS:
				case OPEN_PAREN:
				case OPEN_BRACK:
				case ADD:
				case MINUS:
				case NOT_OP:
				case OPEN_BRACE:
					{
					setState(1312);
					test();
					setState(1313);
					match(COLON);
					setState(1314);
					test();
					}
					break;
				case POWER:
					{
					setState(1316);
					match(POWER);
					setState(1317);
					expr(0);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1338);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ASYNC:
				case FOR:
					{
					setState(1320);
					comp_for();
					}
					break;
				case COMMA:
				case CLOSE_BRACE:
					{
					setState(1332);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,179,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(1321);
							match(COMMA);
							setState(1328);
							_errHandler.sync(this);
							switch (_input.LA(1)) {
							case STRING:
							case NUMBER:
							case AWAIT:
							case FALSE:
							case LAMBDA:
							case MATCH:
							case NONE:
							case NOT:
							case TRUE:
							case UNDERSCORE:
							case NAME:
							case ELLIPSIS:
							case OPEN_PAREN:
							case OPEN_BRACK:
							case ADD:
							case MINUS:
							case NOT_OP:
							case OPEN_BRACE:
								{
								setState(1322);
								test();
								setState(1323);
								match(COLON);
								setState(1324);
								test();
								}
								break;
							case POWER:
								{
								setState(1326);
								match(POWER);
								setState(1327);
								expr(0);
								}
								break;
							default:
								throw new NoViableAltException(this);
							}
							}
							}
						}
						setState(1334);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,179,_ctx);
					}
					setState(1336);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==COMMA) {
						{
						setState(1335);
						match(COMMA);
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				break;
			case 2:
				{
				{
				setState(1342);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case NUMBER:
				case AWAIT:
				case FALSE:
				case LAMBDA:
				case MATCH:
				case NONE:
				case NOT:
				case TRUE:
				case UNDERSCORE:
				case NAME:
				case ELLIPSIS:
				case OPEN_PAREN:
				case OPEN_BRACK:
				case ADD:
				case MINUS:
				case NOT_OP:
				case OPEN_BRACE:
					{
					setState(1340);
					test();
					}
					break;
				case STAR:
					{
					setState(1341);
					star_expr();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1358);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ASYNC:
				case FOR:
					{
					setState(1344);
					comp_for();
					}
					break;
				case COMMA:
				case CLOSE_BRACE:
					{
					setState(1352);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,184,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(1345);
							match(COMMA);
							setState(1348);
							_errHandler.sync(this);
							switch (_input.LA(1)) {
							case STRING:
							case NUMBER:
							case AWAIT:
							case FALSE:
							case LAMBDA:
							case MATCH:
							case NONE:
							case NOT:
							case TRUE:
							case UNDERSCORE:
							case NAME:
							case ELLIPSIS:
							case OPEN_PAREN:
							case OPEN_BRACK:
							case ADD:
							case MINUS:
							case NOT_OP:
							case OPEN_BRACE:
								{
								setState(1346);
								test();
								}
								break;
							case STAR:
								{
								setState(1347);
								star_expr();
								}
								break;
							default:
								throw new NoViableAltException(this);
							}
							}
							}
						}
						setState(1354);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,184,_ctx);
					}
					setState(1356);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==COMMA) {
						{
						setState(1355);
						match(COMMA);
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ClassdefContext extends ParserRuleContext {
		public TerminalNode CLASS() { return getToken(Python3Parser.CLASS, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode COLON() { return getToken(Python3Parser.COLON, 0); }
		public BlockContext block() {
			return getRuleContext(BlockContext.class,0);
		}
		public TerminalNode OPEN_PAREN() { return getToken(Python3Parser.OPEN_PAREN, 0); }
		public TerminalNode CLOSE_PAREN() { return getToken(Python3Parser.CLOSE_PAREN, 0); }
		public ArglistContext arglist() {
			return getRuleContext(ArglistContext.class,0);
		}
		public ClassdefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_classdef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterClassdef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitClassdef(this);
		}
	}

	public final ClassdefContext classdef() throws RecognitionException {
		ClassdefContext _localctx = new ClassdefContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_classdef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1362);
			match(CLASS);
			setState(1363);
			name();
			setState(1369);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPEN_PAREN) {
				{
				setState(1364);
				match(OPEN_PAREN);
				setState(1366);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4863924168670839832L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
					{
					setState(1365);
					arglist();
					}
				}

				setState(1368);
				match(CLOSE_PAREN);
				}
			}

			setState(1371);
			match(COLON);
			setState(1372);
			block();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArglistContext extends ParserRuleContext {
		public List<ArgumentContext> argument() {
			return getRuleContexts(ArgumentContext.class);
		}
		public ArgumentContext argument(int i) {
			return getRuleContext(ArgumentContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(Python3Parser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(Python3Parser.COMMA, i);
		}
		public ArglistContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arglist; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterArglist(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitArglist(this);
		}
	}

	public final ArglistContext arglist() throws RecognitionException {
		ArglistContext _localctx = new ArglistContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_arglist);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1374);
			argument();
			setState(1379);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,190,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1375);
					match(COMMA);
					setState(1376);
					argument();
					}
					}
				}
				setState(1381);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,190,_ctx);
			}
			setState(1383);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(1382);
				match(COMMA);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArgumentContext extends ParserRuleContext {
		public List<TestContext> test() {
			return getRuleContexts(TestContext.class);
		}
		public TestContext test(int i) {
			return getRuleContext(TestContext.class,i);
		}
		public TerminalNode ASSIGN() { return getToken(Python3Parser.ASSIGN, 0); }
		public TerminalNode POWER() { return getToken(Python3Parser.POWER, 0); }
		public TerminalNode STAR() { return getToken(Python3Parser.STAR, 0); }
		public Comp_forContext comp_for() {
			return getRuleContext(Comp_forContext.class,0);
		}
		public ArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_argument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitArgument(this);
		}
	}

	public final ArgumentContext argument() throws RecognitionException {
		ArgumentContext _localctx = new ArgumentContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_argument);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1397);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
			case 1:
				{
				setState(1385);
				test();
				setState(1387);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ASYNC || _la==FOR) {
					{
					setState(1386);
					comp_for();
					}
				}

				}
				break;
			case 2:
				{
				setState(1389);
				test();
				setState(1390);
				match(ASSIGN);
				setState(1391);
				test();
				}
				break;
			case 3:
				{
				setState(1393);
				match(POWER);
				setState(1394);
				test();
				}
				break;
			case 4:
				{
				setState(1395);
				match(STAR);
				setState(1396);
				test();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Comp_iterContext extends ParserRuleContext {
		public Comp_forContext comp_for() {
			return getRuleContext(Comp_forContext.class,0);
		}
		public Comp_ifContext comp_if() {
			return getRuleContext(Comp_ifContext.class,0);
		}
		public Comp_iterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_iter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterComp_iter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitComp_iter(this);
		}
	}

	public final Comp_iterContext comp_iter() throws RecognitionException {
		Comp_iterContext _localctx = new Comp_iterContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_comp_iter);
		try {
			setState(1401);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ASYNC:
			case FOR:
				enterOuterAlt(_localctx, 1);
				{
				setState(1399);
				comp_for();
				}
				break;
			case IF:
				enterOuterAlt(_localctx, 2);
				{
				setState(1400);
				comp_if();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Comp_forContext extends ParserRuleContext {
		public TerminalNode FOR() { return getToken(Python3Parser.FOR, 0); }
		public ExprlistContext exprlist() {
			return getRuleContext(ExprlistContext.class,0);
		}
		public TerminalNode IN() { return getToken(Python3Parser.IN, 0); }
		public Or_testContext or_test() {
			return getRuleContext(Or_testContext.class,0);
		}
		public TerminalNode ASYNC() { return getToken(Python3Parser.ASYNC, 0); }
		public Comp_iterContext comp_iter() {
			return getRuleContext(Comp_iterContext.class,0);
		}
		public Comp_forContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_for; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterComp_for(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitComp_for(this);
		}
	}

	public final Comp_forContext comp_for() throws RecognitionException {
		Comp_forContext _localctx = new Comp_forContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_comp_for);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1404);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASYNC) {
				{
				setState(1403);
				match(ASYNC);
				}
			}

			setState(1406);
			match(FOR);
			setState(1407);
			exprlist();
			setState(1408);
			match(IN);
			setState(1409);
			or_test();
			setState(1411);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 37749248L) != 0)) {
				{
				setState(1410);
				comp_iter();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Comp_ifContext extends ParserRuleContext {
		public TerminalNode IF() { return getToken(Python3Parser.IF, 0); }
		public Test_nocondContext test_nocond() {
			return getRuleContext(Test_nocondContext.class,0);
		}
		public Comp_iterContext comp_iter() {
			return getRuleContext(Comp_iterContext.class,0);
		}
		public Comp_ifContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_if; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterComp_if(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitComp_if(this);
		}
	}

	public final Comp_ifContext comp_if() throws RecognitionException {
		Comp_ifContext _localctx = new Comp_ifContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_comp_if);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1413);
			match(IF);
			setState(1414);
			test_nocond();
			setState(1416);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 37749248L) != 0)) {
				{
				setState(1415);
				comp_iter();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Encoding_declContext extends ParserRuleContext {
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Encoding_declContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_encoding_decl; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterEncoding_decl(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitEncoding_decl(this);
		}
	}

	public final Encoding_declContext encoding_decl() throws RecognitionException {
		Encoding_declContext _localctx = new Encoding_declContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_encoding_decl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1418);
			name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Yield_exprContext extends ParserRuleContext {
		public TerminalNode YIELD() { return getToken(Python3Parser.YIELD, 0); }
		public Yield_argContext yield_arg() {
			return getRuleContext(Yield_argContext.class,0);
		}
		public Yield_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_yield_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterYield_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitYield_expr(this);
		}
	}

	public final Yield_exprContext yield_expr() throws RecognitionException {
		Yield_exprContext _localctx = new Yield_exprContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_yield_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1420);
			match(YIELD);
			setState(1422);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 180180556213912600L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 12673L) != 0)) {
				{
				setState(1421);
				yield_arg();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Yield_argContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(Python3Parser.FROM, 0); }
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public TestlistContext testlist() {
			return getRuleContext(TestlistContext.class,0);
		}
		public Yield_argContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_yield_arg; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterYield_arg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitYield_arg(this);
		}
	}

	public final Yield_argContext yield_arg() throws RecognitionException {
		Yield_argContext _localctx = new Yield_argContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_yield_arg);
		try {
			setState(1427);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FROM:
				enterOuterAlt(_localctx, 1);
				{
				setState(1424);
				match(FROM);
				setState(1425);
				test();
				}
				break;
			case STRING:
			case NUMBER:
			case AWAIT:
			case FALSE:
			case LAMBDA:
			case MATCH:
			case NONE:
			case NOT:
			case TRUE:
			case UNDERSCORE:
			case NAME:
			case ELLIPSIS:
			case OPEN_PAREN:
			case OPEN_BRACK:
			case ADD:
			case MINUS:
			case NOT_OP:
			case OPEN_BRACE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1426);
				testlist();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StringsContext extends ParserRuleContext {
		public List<TerminalNode> STRING() { return getTokens(Python3Parser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(Python3Parser.STRING, i);
		}
		public StringsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strings; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).enterStrings(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof Python3ParserListener ) ((Python3ParserListener)listener).exitStrings(this);
		}
	}

	public final StringsContext strings() throws RecognitionException {
		StringsContext _localctx = new StringsContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_strings);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1430);
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(1429);
				match(STRING);
				}
				}
				setState(1432);
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==STRING );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 60:
			return literal_pattern_sempred((Literal_patternContext)_localctx, predIndex);
		case 61:
			return literal_expr_sempred((Literal_exprContext)_localctx, predIndex);
		case 68:
			return pattern_capture_target_sempred((Pattern_capture_targetContext)_localctx, predIndex);
		case 70:
			return value_pattern_sempred((Value_patternContext)_localctx, predIndex);
		case 97:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean literal_pattern_sempred(Literal_patternContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return  this.CannotBePlusMinus() ;
		}
		return true;
	}
	private boolean literal_expr_sempred(Literal_exprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return  this.CannotBePlusMinus() ;
		}
		return true;
	}
	private boolean pattern_capture_target_sempred(Pattern_capture_targetContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return  this.CannotBeDotLpEq() ;
		}
		return true;
	}
	private boolean value_pattern_sempred(Value_patternContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return  this.CannotBeDotLpEq() ;
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 4:
			return precpred(_ctx, 8);
		case 5:
			return precpred(_ctx, 6);
		case 6:
			return precpred(_ctx, 5);
		case 7:
			return precpred(_ctx, 4);
		case 8:
			return precpred(_ctx, 3);
		case 9:
			return precpred(_ctx, 2);
		case 10:
			return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001f\u059b\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
		"\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
		"\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
		"\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
		"\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007\"\u0002"+
		"#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007\'\u0002"+
		"(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007,\u0002"+
		"-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u00071\u0002"+
		"2\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u00076\u0002"+
		"7\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007;\u0002"+
		"<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007@\u0002"+
		"A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007E\u0002"+
		"F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007J\u0002"+
		"K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007O\u0002"+
		"P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007T\u0002"+
		"U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007Y\u0002"+
		"Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007^\u0002"+
		"_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007c\u0002"+
		"d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007h\u0002"+
		"i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007m\u0002"+
		"n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007r\u0002"+
		"s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0001\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000\u00f4\b\u0000\u0001\u0001"+
		"\u0001\u0001\u0005\u0001\u00f8\b\u0001\n\u0001\f\u0001\u00fb\t\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0005\u0002\u0101\b\u0002\n"+
		"\u0002\f\u0002\u0104\t\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u010c\b\u0003\u0001\u0003\u0003"+
		"\u0003\u010f\b\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0004\u0004\u0114"+
		"\b\u0004\u000b\u0004\f\u0004\u0115\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u011c\b\u0005\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u0126\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0003"+
		"\b\u012d\b\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001\t\u0003\t\u0134\b\t"+
		"\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u013a\b\t\u0005\t\u013c\b\t\n"+
		"\t\f\t\u013f\t\t\u0001\t\u0001\t\u0001\t\u0003\t\u0144\b\t\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0003\t\u014a\b\t\u0005\t\u014c\b\t\n\t\f\t\u014f\t"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u0155\b\t\u0003\t\u0157\b\t"+
		"\u0003\t\u0159\b\t\u0001\t\u0001\t\u0001\t\u0003\t\u015e\b\t\u0003\t\u0160"+
		"\b\t\u0003\t\u0162\b\t\u0001\t\u0001\t\u0003\t\u0166\b\t\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0003\t\u016c\b\t\u0005\t\u016e\b\t\n\t\f\t\u0171\t"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u0177\b\t\u0003\t\u0179\b\t"+
		"\u0003\t\u017b\b\t\u0001\t\u0001\t\u0001\t\u0003\t\u0180\b\t\u0003\t\u0182"+
		"\b\t\u0001\n\u0001\n\u0001\n\u0003\n\u0187\b\n\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0003\u000b\u018c\b\u000b\u0001\u000b\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0003\u000b\u0192\b\u000b\u0005\u000b\u0194\b\u000b\n\u000b"+
		"\f\u000b\u0197\t\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b"+
		"\u019c\b\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b"+
		"\u01a2\b\u000b\u0005\u000b\u01a4\b\u000b\n\u000b\f\u000b\u01a7\t\u000b"+
		"\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u01ad\b\u000b"+
		"\u0003\u000b\u01af\b\u000b\u0003\u000b\u01b1\b\u000b\u0001\u000b\u0001"+
		"\u000b\u0001\u000b\u0003\u000b\u01b6\b\u000b\u0003\u000b\u01b8\b\u000b"+
		"\u0003\u000b\u01ba\b\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u01be\b"+
		"\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u01c4"+
		"\b\u000b\u0005\u000b\u01c6\b\u000b\n\u000b\f\u000b\u01c9\t\u000b\u0001"+
		"\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u01cf\b\u000b\u0003"+
		"\u000b\u01d1\b\u000b\u0003\u000b\u01d3\b\u000b\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0003\u000b\u01d8\b\u000b\u0003\u000b\u01da\b\u000b\u0001"+
		"\f\u0001\f\u0001\r\u0001\r\u0003\r\u01e0\b\r\u0001\u000e\u0001\u000e\u0001"+
		"\u000e\u0005\u000e\u01e5\b\u000e\n\u000e\f\u000e\u01e8\t\u000e\u0001\u000e"+
		"\u0003\u000e\u01eb\b\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f"+
		"\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f"+
		"\u0003\u000f\u01f7\b\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010"+
		"\u0001\u0010\u0003\u0010\u01fe\b\u0010\u0001\u0010\u0001\u0010\u0001\u0010"+
		"\u0003\u0010\u0203\b\u0010\u0005\u0010\u0205\b\u0010\n\u0010\f\u0010\u0208"+
		"\t\u0010\u0003\u0010\u020a\b\u0010\u0001\u0011\u0001\u0011\u0001\u0011"+
		"\u0001\u0011\u0003\u0011\u0210\b\u0011\u0001\u0012\u0001\u0012\u0003\u0012"+
		"\u0214\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0219\b"+
		"\u0012\u0005\u0012\u021b\b\u0012\n\u0012\f\u0012\u021e\t\u0012\u0001\u0012"+
		"\u0003\u0012\u0221\b\u0012\u0001\u0013\u0001\u0013\u0001\u0014\u0001\u0014"+
		"\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016"+
		"\u0001\u0016\u0001\u0016\u0003\u0016\u022f\b\u0016\u0001\u0017\u0001\u0017"+
		"\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0003\u0019\u0237\b\u0019"+
		"\u0001\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b"+
		"\u0003\u001b\u023f\b\u001b\u0003\u001b\u0241\b\u001b\u0001\u001c\u0001"+
		"\u001c\u0003\u001c\u0245\b\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001e\u0001\u001e\u0005\u001e\u024c\b\u001e\n\u001e\f\u001e\u024f\t\u001e"+
		"\u0001\u001e\u0001\u001e\u0004\u001e\u0253\b\u001e\u000b\u001e\f\u001e"+
		"\u0254\u0003\u001e\u0257\b\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0001"+
		"\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u0260\b\u001e\u0001"+
		"\u001f\u0001\u001f\u0001\u001f\u0003\u001f\u0265\b\u001f\u0001 \u0001"+
		" \u0001 \u0003 \u026a\b \u0001!\u0001!\u0001!\u0005!\u026f\b!\n!\f!\u0272"+
		"\t!\u0001!\u0003!\u0275\b!\u0001\"\u0001\"\u0001\"\u0005\"\u027a\b\"\n"+
		"\"\f\"\u027d\t\"\u0001#\u0001#\u0001#\u0005#\u0282\b#\n#\f#\u0285\t#\u0001"+
		"$\u0001$\u0001$\u0001$\u0005$\u028b\b$\n$\f$\u028e\t$\u0001%\u0001%\u0001"+
		"%\u0001%\u0005%\u0294\b%\n%\f%\u0297\t%\u0001&\u0001&\u0001&\u0001&\u0003"+
		"&\u029d\b&\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001"+
		"\'\u0001\'\u0001\'\u0003\'\u02a9\b\'\u0001(\u0001(\u0001(\u0001(\u0003"+
		"(\u02af\b(\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001"+
		")\u0005)\u02ba\b)\n)\f)\u02bd\t)\u0001)\u0001)\u0001)\u0003)\u02c2\b)"+
		"\u0001*\u0001*\u0001*\u0001*\u0001*\u0001*\u0001*\u0003*\u02cb\b*\u0001"+
		"+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0003+\u02d6"+
		"\b+\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0004,\u02df\b,\u000b"+
		",\f,\u02e0\u0001,\u0001,\u0001,\u0003,\u02e6\b,\u0001,\u0001,\u0001,\u0003"+
		",\u02eb\b,\u0001,\u0001,\u0001,\u0003,\u02f0\b,\u0001-\u0001-\u0001-\u0001"+
		"-\u0005-\u02f6\b-\n-\f-\u02f9\t-\u0001-\u0001-\u0001-\u0001.\u0001.\u0001"+
		".\u0003.\u0301\b.\u0001/\u0001/\u0001/\u0001/\u0003/\u0307\b/\u0003/\u0309"+
		"\b/\u00010\u00010\u00010\u00010\u00040\u030f\b0\u000b0\f0\u0310\u0001"+
		"0\u00010\u00030\u0315\b0\u00011\u00011\u00011\u00011\u00011\u00011\u0004"+
		"1\u031d\b1\u000b1\f1\u031e\u00011\u00011\u00012\u00012\u00012\u00032\u0326"+
		"\b2\u00012\u00032\u0329\b2\u00013\u00013\u00043\u032d\b3\u000b3\f3\u032e"+
		"\u00013\u00033\u0332\b3\u00014\u00014\u00014\u00034\u0337\b4\u00015\u0001"+
		"5\u00015\u00035\u033c\b5\u00015\u00015\u00015\u00016\u00016\u00016\u0001"+
		"7\u00017\u00037\u0346\b7\u00018\u00018\u00038\u034a\b8\u00019\u00019\u0001"+
		"9\u00019\u0001:\u0001:\u0001:\u0005:\u0353\b:\n:\f:\u0356\t:\u0001;\u0001"+
		";\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0003;\u0360\b;\u0001<\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u036a\b<\u0001=\u0001"+
		"=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0003=\u0374\b=\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003>\u037e\b>\u0001?\u0001"+
		"?\u0001?\u0003?\u0383\b?\u0001@\u0001@\u0001@\u0003@\u0388\b@\u0001A\u0001"+
		"A\u0001B\u0001B\u0001C\u0001C\u0001D\u0001D\u0001D\u0001E\u0001E\u0001"+
		"F\u0001F\u0001F\u0001G\u0001G\u0001G\u0004G\u039b\bG\u000bG\fG\u039c\u0001"+
		"H\u0001H\u0003H\u03a1\bH\u0001I\u0001I\u0001I\u0001I\u0001J\u0001J\u0003"+
		"J\u03a9\bJ\u0001J\u0001J\u0001J\u0003J\u03ae\bJ\u0001J\u0003J\u03b1\b"+
		"J\u0001K\u0001K\u0001K\u0003K\u03b6\bK\u0001L\u0001L\u0001L\u0005L\u03bb"+
		"\bL\nL\fL\u03be\tL\u0001L\u0003L\u03c1\bL\u0001M\u0001M\u0003M\u03c5\b"+
		"M\u0001N\u0001N\u0001N\u0001N\u0003N\u03cb\bN\u0001O\u0001O\u0001O\u0001"+
		"O\u0001O\u0003O\u03d2\bO\u0001O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001"+
		"O\u0003O\u03db\bO\u0001O\u0001O\u0001O\u0001O\u0001O\u0003O\u03e2\bO\u0001"+
		"O\u0001O\u0003O\u03e6\bO\u0001P\u0001P\u0001P\u0005P\u03eb\bP\nP\fP\u03ee"+
		"\tP\u0001Q\u0001Q\u0003Q\u03f2\bQ\u0001Q\u0001Q\u0001Q\u0001R\u0001R\u0001"+
		"R\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0003S\u0402"+
		"\bS\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0003S\u040a\bS\u0001S\u0001"+
		"S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0003S\u0414\bS\u0001S\u0001"+
		"S\u0003S\u0418\bS\u0001T\u0001T\u0001T\u0005T\u041d\bT\nT\fT\u0420\tT"+
		"\u0001U\u0001U\u0001U\u0005U\u0425\bU\nU\fU\u0428\tU\u0001V\u0001V\u0001"+
		"V\u0001V\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0003W\u0434\bW\u0001"+
		"W\u0003W\u0437\bW\u0001X\u0001X\u0003X\u043b\bX\u0001Y\u0001Y\u0003Y\u043f"+
		"\bY\u0001Y\u0001Y\u0001Y\u0001Z\u0001Z\u0003Z\u0446\bZ\u0001Z\u0001Z\u0001"+
		"Z\u0001[\u0001[\u0001[\u0005[\u044e\b[\n[\f[\u0451\t[\u0001\\\u0001\\"+
		"\u0001\\\u0005\\\u0456\b\\\n\\\f\\\u0459\t\\\u0001]\u0001]\u0001]\u0003"+
		"]\u045e\b]\u0001^\u0001^\u0001^\u0001^\u0005^\u0464\b^\n^\f^\u0467\t^"+
		"\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001"+
		"_\u0001_\u0001_\u0001_\u0003_\u0476\b_\u0001`\u0001`\u0001`\u0001a\u0001"+
		"a\u0001a\u0004a\u047e\ba\u000ba\fa\u047f\u0001a\u0003a\u0483\ba\u0001"+
		"a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001"+
		"a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001"+
		"a\u0005a\u049a\ba\na\fa\u049d\ta\u0001b\u0003b\u04a0\bb\u0001b\u0001b"+
		"\u0005b\u04a4\bb\nb\fb\u04a7\tb\u0001c\u0001c\u0001c\u0003c\u04ac\bc\u0001"+
		"c\u0001c\u0001c\u0003c\u04b1\bc\u0001c\u0001c\u0001c\u0003c\u04b6\bc\u0001"+
		"c\u0001c\u0001c\u0001c\u0004c\u04bc\bc\u000bc\fc\u04bd\u0001c\u0001c\u0001"+
		"c\u0001c\u0003c\u04c4\bc\u0001d\u0001d\u0001e\u0001e\u0003e\u04ca\be\u0001"+
		"e\u0001e\u0001e\u0001e\u0003e\u04d0\be\u0005e\u04d2\be\ne\fe\u04d5\te"+
		"\u0001e\u0003e\u04d8\be\u0003e\u04da\be\u0001f\u0001f\u0003f\u04de\bf"+
		"\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0003f\u04e7\bf\u0001"+
		"g\u0001g\u0001g\u0005g\u04ec\bg\ng\fg\u04ef\tg\u0001g\u0003g\u04f2\bg"+
		"\u0001h\u0001h\u0003h\u04f6\bh\u0001h\u0001h\u0003h\u04fa\bh\u0001h\u0003"+
		"h\u04fd\bh\u0003h\u04ff\bh\u0001i\u0001i\u0003i\u0503\bi\u0001j\u0001"+
		"j\u0003j\u0507\bj\u0001j\u0001j\u0001j\u0003j\u050c\bj\u0005j\u050e\b"+
		"j\nj\fj\u0511\tj\u0001j\u0003j\u0514\bj\u0001k\u0001k\u0001k\u0005k\u0519"+
		"\bk\nk\fk\u051c\tk\u0001k\u0003k\u051f\bk\u0001l\u0001l\u0001l\u0001l"+
		"\u0001l\u0001l\u0003l\u0527\bl\u0001l\u0001l\u0001l\u0001l\u0001l\u0001"+
		"l\u0001l\u0001l\u0003l\u0531\bl\u0005l\u0533\bl\nl\fl\u0536\tl\u0001l"+
		"\u0003l\u0539\bl\u0003l\u053b\bl\u0001l\u0001l\u0003l\u053f\bl\u0001l"+
		"\u0001l\u0001l\u0001l\u0003l\u0545\bl\u0005l\u0547\bl\nl\fl\u054a\tl\u0001"+
		"l\u0003l\u054d\bl\u0003l\u054f\bl\u0003l\u0551\bl\u0001m\u0001m\u0001"+
		"m\u0001m\u0003m\u0557\bm\u0001m\u0003m\u055a\bm\u0001m\u0001m\u0001m\u0001"+
		"n\u0001n\u0001n\u0005n\u0562\bn\nn\fn\u0565\tn\u0001n\u0003n\u0568\bn"+
		"\u0001o\u0001o\u0003o\u056c\bo\u0001o\u0001o\u0001o\u0001o\u0001o\u0001"+
		"o\u0001o\u0001o\u0003o\u0576\bo\u0001p\u0001p\u0003p\u057a\bp\u0001q\u0003"+
		"q\u057d\bq\u0001q\u0001q\u0001q\u0001q\u0001q\u0003q\u0584\bq\u0001r\u0001"+
		"r\u0001r\u0003r\u0589\br\u0001s\u0001s\u0001t\u0001t\u0003t\u058f\bt\u0001"+
		"u\u0001u\u0001u\u0003u\u0594\bu\u0001v\u0004v\u0597\bv\u000bv\fv\u0598"+
		"\u0001v\u0000\u0001\u00c2w\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
		"\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\"+
		"^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090"+
		"\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8"+
		"\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0"+
		"\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8"+
		"\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u0000\u0007"+
		"\u0001\u0000Xd\u0001\u000067\u0002\u0000GHLL\u0003\u000088IKVV\u0001\u0000"+
		"GH\u0001\u0000EF\u0003\u0000\u001e\u001e((--\u0632\u0000\u00f3\u0001\u0000"+
		"\u0000\u0000\u0002\u00f9\u0001\u0000\u0000\u0000\u0004\u00fe\u0001\u0000"+
		"\u0000\u0000\u0006\u0107\u0001\u0000\u0000\u0000\b\u0113\u0001\u0000\u0000"+
		"\u0000\n\u0117\u0001\u0000\u0000\u0000\f\u011d\u0001\u0000\u0000\u0000"+
		"\u000e\u0120\u0001\u0000\u0000\u0000\u0010\u012a\u0001\u0000\u0000\u0000"+
		"\u0012\u0181\u0001\u0000\u0000\u0000\u0014\u0183\u0001\u0000\u0000\u0000"+
		"\u0016\u01d9\u0001\u0000\u0000\u0000\u0018\u01db\u0001\u0000\u0000\u0000"+
		"\u001a\u01df\u0001\u0000\u0000\u0000\u001c\u01e1\u0001\u0000\u0000\u0000"+
		"\u001e\u01f6\u0001\u0000\u0000\u0000 \u01f8\u0001\u0000\u0000\u0000\""+
		"\u020b\u0001\u0000\u0000\u0000$\u0213\u0001\u0000\u0000\u0000&\u0222\u0001"+
		"\u0000\u0000\u0000(\u0224\u0001\u0000\u0000\u0000*\u0227\u0001\u0000\u0000"+
		"\u0000,\u022e\u0001\u0000\u0000\u0000.\u0230\u0001\u0000\u0000\u00000"+
		"\u0232\u0001\u0000\u0000\u00002\u0234\u0001\u0000\u0000\u00004\u0238\u0001"+
		"\u0000\u0000\u00006\u023a\u0001\u0000\u0000\u00008\u0244\u0001\u0000\u0000"+
		"\u0000:\u0246\u0001\u0000\u0000\u0000<\u0249\u0001\u0000\u0000\u0000>"+
		"\u0261\u0001\u0000\u0000\u0000@\u0266\u0001\u0000\u0000\u0000B\u026b\u0001"+
		"\u0000\u0000\u0000D\u0276\u0001\u0000\u0000\u0000F\u027e\u0001\u0000\u0000"+
		"\u0000H\u0286\u0001\u0000\u0000\u0000J\u028f\u0001\u0000\u0000\u0000L"+
		"\u0298\u0001\u0000\u0000\u0000N\u02a8\u0001\u0000\u0000\u0000P\u02aa\u0001"+
		"\u0000\u0000\u0000R\u02b0\u0001\u0000\u0000\u0000T\u02c3\u0001\u0000\u0000"+
		"\u0000V\u02cc\u0001\u0000\u0000\u0000X\u02d7\u0001\u0000\u0000\u0000Z"+
		"\u02f1\u0001\u0000\u0000\u0000\\\u02fd\u0001\u0000\u0000\u0000^\u0302"+
		"\u0001\u0000\u0000\u0000`\u0314\u0001\u0000\u0000\u0000b\u0316\u0001\u0000"+
		"\u0000\u0000d\u0328\u0001\u0000\u0000\u0000f\u032a\u0001\u0000\u0000\u0000"+
		"h\u0336\u0001\u0000\u0000\u0000j\u0338\u0001\u0000\u0000\u0000l\u0340"+
		"\u0001\u0000\u0000\u0000n\u0345\u0001\u0000\u0000\u0000p\u0349\u0001\u0000"+
		"\u0000\u0000r\u034b\u0001\u0000\u0000\u0000t\u034f\u0001\u0000\u0000\u0000"+
		"v\u035f\u0001\u0000\u0000\u0000x\u0369\u0001\u0000\u0000\u0000z\u0373"+
		"\u0001\u0000\u0000\u0000|\u037d\u0001\u0000\u0000\u0000~\u0382\u0001\u0000"+
		"\u0000\u0000\u0080\u0387\u0001\u0000\u0000\u0000\u0082\u0389\u0001\u0000"+
		"\u0000\u0000\u0084\u038b\u0001\u0000\u0000\u0000\u0086\u038d\u0001\u0000"+
		"\u0000\u0000\u0088\u038f\u0001\u0000\u0000\u0000\u008a\u0392\u0001\u0000"+
		"\u0000\u0000\u008c\u0394\u0001\u0000\u0000\u0000\u008e\u0397\u0001\u0000"+
		"\u0000\u0000\u0090\u03a0\u0001\u0000\u0000\u0000\u0092\u03a2\u0001\u0000"+
		"\u0000\u0000\u0094\u03b0\u0001\u0000\u0000\u0000\u0096\u03b2\u0001\u0000"+
		"\u0000\u0000\u0098\u03b7\u0001\u0000\u0000\u0000\u009a\u03c4\u0001\u0000"+
		"\u0000\u0000\u009c\u03ca\u0001\u0000\u0000\u0000\u009e\u03e5\u0001\u0000"+
		"\u0000\u0000\u00a0\u03e7\u0001\u0000\u0000\u0000\u00a2\u03f1\u0001\u0000"+
		"\u0000\u0000\u00a4\u03f6\u0001\u0000\u0000\u0000\u00a6\u0417\u0001\u0000"+
		"\u0000\u0000\u00a8\u0419\u0001\u0000\u0000\u0000\u00aa\u0421\u0001\u0000"+
		"\u0000\u0000\u00ac\u0429\u0001\u0000\u0000\u0000\u00ae\u0436\u0001\u0000"+
		"\u0000\u0000\u00b0\u043a\u0001\u0000\u0000\u0000\u00b2\u043c\u0001\u0000"+
		"\u0000\u0000\u00b4\u0443\u0001\u0000\u0000\u0000\u00b6\u044a\u0001\u0000"+
		"\u0000\u0000\u00b8\u0452\u0001\u0000\u0000\u0000\u00ba\u045d\u0001\u0000"+
		"\u0000\u0000\u00bc\u045f\u0001\u0000\u0000\u0000\u00be\u0475\u0001\u0000"+
		"\u0000\u0000\u00c0\u0477\u0001\u0000\u0000\u0000\u00c2\u0482\u0001\u0000"+
		"\u0000\u0000\u00c4\u049f\u0001\u0000\u0000\u0000\u00c6\u04c3\u0001\u0000"+
		"\u0000\u0000\u00c8\u04c5\u0001\u0000\u0000\u0000\u00ca\u04c9\u0001\u0000"+
		"\u0000\u0000\u00cc\u04e6\u0001\u0000\u0000\u0000\u00ce\u04e8\u0001\u0000"+
		"\u0000\u0000\u00d0\u04fe\u0001\u0000\u0000\u0000\u00d2\u0500\u0001\u0000"+
		"\u0000\u0000\u00d4\u0506\u0001\u0000\u0000\u0000\u00d6\u0515\u0001\u0000"+
		"\u0000\u0000\u00d8\u0550\u0001\u0000\u0000\u0000\u00da\u0552\u0001\u0000"+
		"\u0000\u0000\u00dc\u055e\u0001\u0000\u0000\u0000\u00de\u0575\u0001\u0000"+
		"\u0000\u0000\u00e0\u0579\u0001\u0000\u0000\u0000\u00e2\u057c\u0001\u0000"+
		"\u0000\u0000\u00e4\u0585\u0001\u0000\u0000\u0000\u00e6\u058a\u0001\u0000"+
		"\u0000\u0000\u00e8\u058c\u0001\u0000\u0000\u0000\u00ea\u0593\u0001\u0000"+
		"\u0000\u0000\u00ec\u0596\u0001\u0000\u0000\u0000\u00ee\u00f4\u0005,\u0000"+
		"\u0000\u00ef\u00f4\u0003\u001c\u000e\u0000\u00f0\u00f1\u0003N\'\u0000"+
		"\u00f1\u00f2\u0005,\u0000\u0000\u00f2\u00f4\u0001\u0000\u0000\u0000\u00f3"+
		"\u00ee\u0001\u0000\u0000\u0000\u00f3\u00ef\u0001\u0000\u0000\u0000\u00f3"+
		"\u00f0\u0001\u0000\u0000\u0000\u00f4\u0001\u0001\u0000\u0000\u0000\u00f5"+
		"\u00f8\u0005,\u0000\u0000\u00f6\u00f8\u0003\u001a\r\u0000\u00f7\u00f5"+
		"\u0001\u0000\u0000\u0000\u00f7\u00f6\u0001\u0000\u0000\u0000\u00f8\u00fb"+
		"\u0001\u0000\u0000\u0000\u00f9\u00f7\u0001\u0000\u0000\u0000\u00f9\u00fa"+
		"\u0001\u0000\u0000\u0000\u00fa\u00fc\u0001\u0000\u0000\u0000\u00fb\u00f9"+
		"\u0001\u0000\u0000\u0000\u00fc\u00fd\u0005\u0000\u0000\u0001\u00fd\u0003"+
		"\u0001\u0000\u0000\u0000\u00fe\u0102\u0003\u00d6k\u0000\u00ff\u0101\u0005"+
		",\u0000\u0000\u0100\u00ff\u0001\u0000\u0000\u0000\u0101\u0104\u0001\u0000"+
		"\u0000\u0000\u0102\u0100\u0001\u0000\u0000\u0000\u0102\u0103\u0001\u0000"+
		"\u0000\u0000\u0103\u0105\u0001\u0000\u0000\u0000\u0104\u0102\u0001\u0000"+
		"\u0000\u0000\u0105\u0106\u0005\u0000\u0000\u0001\u0106\u0005\u0001\u0000"+
		"\u0000\u0000\u0107\u0108\u0005V\u0000\u0000\u0108\u010e\u0003F#\u0000"+
		"\u0109\u010b\u00059\u0000\u0000\u010a\u010c\u0003\u00dcn\u0000\u010b\u010a"+
		"\u0001\u0000\u0000\u0000\u010b\u010c\u0001\u0000\u0000\u0000\u010c\u010d"+
		"\u0001\u0000\u0000\u0000\u010d\u010f\u0005:\u0000\u0000\u010e\u0109\u0001"+
		"\u0000\u0000\u0000\u010e\u010f\u0001\u0000\u0000\u0000\u010f\u0110\u0001"+
		"\u0000\u0000\u0000\u0110\u0111\u0005,\u0000\u0000\u0111\u0007\u0001\u0000"+
		"\u0000\u0000\u0112\u0114\u0003\u0006\u0003\u0000\u0113\u0112\u0001\u0000"+
		"\u0000\u0000\u0114\u0115\u0001\u0000\u0000\u0000\u0115\u0113\u0001\u0000"+
		"\u0000\u0000\u0115\u0116\u0001\u0000\u0000\u0000\u0116\t\u0001\u0000\u0000"+
		"\u0000\u0117\u011b\u0003\b\u0004\u0000\u0118\u011c\u0003\u00dam\u0000"+
		"\u0119\u011c\u0003\u000e\u0007\u0000\u011a\u011c\u0003\f\u0006\u0000\u011b"+
		"\u0118\u0001\u0000\u0000\u0000\u011b\u0119\u0001\u0000\u0000\u0000\u011b"+
		"\u011a\u0001\u0000\u0000\u0000\u011c\u000b\u0001\u0000\u0000\u0000\u011d"+
		"\u011e\u0005\t\u0000\u0000\u011e\u011f\u0003\u000e\u0007\u0000\u011f\r"+
		"\u0001\u0000\u0000\u0000\u0120\u0121\u0005\u000f\u0000\u0000\u0121\u0122"+
		"\u0003\u00c8d\u0000\u0122\u0125\u0003\u0010\b\u0000\u0123\u0124\u0005"+
		"W\u0000\u0000\u0124\u0126\u0003\u00aeW\u0000\u0125\u0123\u0001\u0000\u0000"+
		"\u0000\u0125\u0126\u0001\u0000\u0000\u0000\u0126\u0127\u0001\u0000\u0000"+
		"\u0000\u0127\u0128\u0005<\u0000\u0000\u0128\u0129\u0003`0\u0000\u0129"+
		"\u000f\u0001\u0000\u0000\u0000\u012a\u012c\u00059\u0000\u0000\u012b\u012d"+
		"\u0003\u0012\t\u0000\u012c\u012b\u0001\u0000\u0000\u0000\u012c\u012d\u0001"+
		"\u0000\u0000\u0000\u012d\u012e\u0001\u0000\u0000\u0000\u012e\u012f\u0005"+
		":\u0000\u0000\u012f\u0011\u0001\u0000\u0000\u0000\u0130\u0133\u0003\u0014"+
		"\n\u0000\u0131\u0132\u0005?\u0000\u0000\u0132\u0134\u0003\u00aeW\u0000"+
		"\u0133\u0131\u0001\u0000\u0000\u0000\u0133\u0134\u0001\u0000\u0000\u0000"+
		"\u0134\u013d\u0001\u0000\u0000\u0000\u0135\u0136\u0005;\u0000\u0000\u0136"+
		"\u0139\u0003\u0014\n\u0000\u0137\u0138\u0005?\u0000\u0000\u0138\u013a"+
		"\u0003\u00aeW\u0000\u0139\u0137\u0001\u0000\u0000\u0000\u0139\u013a\u0001"+
		"\u0000\u0000\u0000\u013a\u013c\u0001\u0000\u0000\u0000\u013b\u0135\u0001"+
		"\u0000\u0000\u0000\u013c\u013f\u0001\u0000\u0000\u0000\u013d\u013b\u0001"+
		"\u0000\u0000\u0000\u013d\u013e\u0001\u0000\u0000\u0000\u013e\u0161\u0001"+
		"\u0000\u0000\u0000\u013f\u013d\u0001\u0000\u0000\u0000\u0140\u015f\u0005"+
		";\u0000\u0000\u0141\u0143\u00058\u0000\u0000\u0142\u0144\u0003\u0014\n"+
		"\u0000\u0143\u0142\u0001\u0000\u0000\u0000\u0143\u0144\u0001\u0000\u0000"+
		"\u0000\u0144\u014d\u0001\u0000\u0000\u0000\u0145\u0146\u0005;\u0000\u0000"+
		"\u0146\u0149\u0003\u0014\n\u0000\u0147\u0148\u0005?\u0000\u0000\u0148"+
		"\u014a\u0003\u00aeW\u0000\u0149\u0147\u0001\u0000\u0000\u0000\u0149\u014a"+
		"\u0001\u0000\u0000\u0000\u014a\u014c\u0001\u0000\u0000\u0000\u014b\u0145"+
		"\u0001\u0000\u0000\u0000\u014c\u014f\u0001\u0000\u0000\u0000\u014d\u014b"+
		"\u0001\u0000\u0000\u0000\u014d\u014e\u0001\u0000\u0000\u0000\u014e\u0158"+
		"\u0001\u0000\u0000\u0000\u014f\u014d\u0001\u0000\u0000\u0000\u0150\u0156"+
		"\u0005;\u0000\u0000\u0151\u0152\u0005>\u0000\u0000\u0152\u0154\u0003\u0014"+
		"\n\u0000\u0153\u0155\u0005;\u0000\u0000\u0154\u0153\u0001\u0000\u0000"+
		"\u0000\u0154\u0155\u0001\u0000\u0000\u0000\u0155\u0157\u0001\u0000\u0000"+
		"\u0000\u0156\u0151\u0001\u0000\u0000\u0000\u0156\u0157\u0001\u0000\u0000"+
		"\u0000\u0157\u0159\u0001\u0000\u0000\u0000\u0158\u0150\u0001\u0000\u0000"+
		"\u0000\u0158\u0159\u0001\u0000\u0000\u0000\u0159\u0160\u0001\u0000\u0000"+
		"\u0000\u015a\u015b\u0005>\u0000\u0000\u015b\u015d\u0003\u0014\n\u0000"+
		"\u015c\u015e\u0005;\u0000\u0000\u015d\u015c\u0001\u0000\u0000\u0000\u015d"+
		"\u015e\u0001\u0000\u0000\u0000\u015e\u0160\u0001\u0000\u0000\u0000\u015f"+
		"\u0141\u0001\u0000\u0000\u0000\u015f\u015a\u0001\u0000\u0000\u0000\u015f"+
		"\u0160\u0001\u0000\u0000\u0000\u0160\u0162\u0001\u0000\u0000\u0000\u0161"+
		"\u0140\u0001\u0000\u0000\u0000\u0161\u0162\u0001\u0000\u0000\u0000\u0162"+
		"\u0182\u0001\u0000\u0000\u0000\u0163\u0165\u00058\u0000\u0000\u0164\u0166"+
		"\u0003\u0014\n\u0000\u0165\u0164\u0001\u0000\u0000\u0000\u0165\u0166\u0001"+
		"\u0000\u0000\u0000\u0166\u016f\u0001\u0000\u0000\u0000\u0167\u0168\u0005"+
		";\u0000\u0000\u0168\u016b\u0003\u0014\n\u0000\u0169\u016a\u0005?\u0000"+
		"\u0000\u016a\u016c\u0003\u00aeW\u0000\u016b\u0169\u0001\u0000\u0000\u0000"+
		"\u016b\u016c\u0001\u0000\u0000\u0000\u016c\u016e\u0001\u0000\u0000\u0000"+
		"\u016d\u0167\u0001\u0000\u0000\u0000\u016e\u0171\u0001\u0000\u0000\u0000"+
		"\u016f\u016d\u0001\u0000\u0000\u0000\u016f\u0170\u0001\u0000\u0000\u0000"+
		"\u0170\u017a\u0001\u0000\u0000\u0000\u0171\u016f\u0001\u0000\u0000\u0000"+
		"\u0172\u0178\u0005;\u0000\u0000\u0173\u0174\u0005>\u0000\u0000\u0174\u0176"+
		"\u0003\u0014\n\u0000\u0175\u0177\u0005;\u0000\u0000\u0176\u0175\u0001"+
		"\u0000\u0000\u0000\u0176\u0177\u0001\u0000\u0000\u0000\u0177\u0179\u0001"+
		"\u0000\u0000\u0000\u0178\u0173\u0001\u0000\u0000\u0000\u0178\u0179\u0001"+
		"\u0000\u0000\u0000\u0179\u017b\u0001\u0000\u0000\u0000\u017a\u0172\u0001"+
		"\u0000\u0000\u0000\u017a\u017b\u0001\u0000\u0000\u0000\u017b\u0182\u0001"+
		"\u0000\u0000\u0000\u017c\u017d\u0005>\u0000\u0000\u017d\u017f\u0003\u0014"+
		"\n\u0000\u017e\u0180\u0005;\u0000\u0000\u017f\u017e\u0001\u0000\u0000"+
		"\u0000\u017f\u0180\u0001\u0000\u0000\u0000\u0180\u0182\u0001\u0000\u0000"+
		"\u0000\u0181\u0130\u0001\u0000\u0000\u0000\u0181\u0163\u0001\u0000\u0000"+
		"\u0000\u0181\u017c\u0001\u0000\u0000\u0000\u0182\u0013\u0001\u0000\u0000"+
		"\u0000\u0183\u0186\u0003\u00c8d\u0000\u0184\u0185\u0005<\u0000\u0000\u0185"+
		"\u0187\u0003\u00aeW\u0000\u0186\u0184\u0001\u0000\u0000\u0000\u0186\u0187"+
		"\u0001\u0000\u0000\u0000\u0187\u0015\u0001\u0000\u0000\u0000\u0188\u018b"+
		"\u0003\u0018\f\u0000\u0189\u018a\u0005?\u0000\u0000\u018a\u018c\u0003"+
		"\u00aeW\u0000\u018b\u0189\u0001\u0000\u0000\u0000\u018b\u018c\u0001\u0000"+
		"\u0000\u0000\u018c\u0195\u0001\u0000\u0000\u0000\u018d\u018e\u0005;\u0000"+
		"\u0000\u018e\u0191\u0003\u0018\f\u0000\u018f\u0190\u0005?\u0000\u0000"+
		"\u0190\u0192\u0003\u00aeW\u0000\u0191\u018f\u0001\u0000\u0000\u0000\u0191"+
		"\u0192\u0001\u0000\u0000\u0000\u0192\u0194\u0001\u0000\u0000\u0000\u0193"+
		"\u018d\u0001\u0000\u0000\u0000\u0194\u0197\u0001\u0000\u0000\u0000\u0195"+
		"\u0193\u0001\u0000\u0000\u0000\u0195\u0196\u0001\u0000\u0000\u0000\u0196"+
		"\u01b9\u0001\u0000\u0000\u0000\u0197\u0195\u0001\u0000\u0000\u0000\u0198"+
		"\u01b7\u0005;\u0000\u0000\u0199\u019b\u00058\u0000\u0000\u019a\u019c\u0003"+
		"\u0018\f\u0000\u019b\u019a\u0001\u0000\u0000\u0000\u019b\u019c\u0001\u0000"+
		"\u0000\u0000\u019c\u01a5\u0001\u0000\u0000\u0000\u019d\u019e\u0005;\u0000"+
		"\u0000\u019e\u01a1\u0003\u0018\f\u0000\u019f\u01a0\u0005?\u0000\u0000"+
		"\u01a0\u01a2\u0003\u00aeW\u0000\u01a1\u019f\u0001\u0000\u0000\u0000\u01a1"+
		"\u01a2\u0001\u0000\u0000\u0000\u01a2\u01a4\u0001\u0000\u0000\u0000\u01a3"+
		"\u019d\u0001\u0000\u0000\u0000\u01a4\u01a7\u0001\u0000\u0000\u0000\u01a5"+
		"\u01a3\u0001\u0000\u0000\u0000\u01a5\u01a6\u0001\u0000\u0000\u0000\u01a6"+
		"\u01b0\u0001\u0000\u0000\u0000\u01a7\u01a5\u0001\u0000\u0000\u0000\u01a8"+
		"\u01ae\u0005;\u0000\u0000\u01a9\u01aa\u0005>\u0000\u0000\u01aa\u01ac\u0003"+
		"\u0018\f\u0000\u01ab\u01ad\u0005;\u0000\u0000\u01ac\u01ab\u0001\u0000"+
		"\u0000\u0000\u01ac\u01ad\u0001\u0000\u0000\u0000\u01ad\u01af\u0001\u0000"+
		"\u0000\u0000\u01ae\u01a9\u0001\u0000\u0000\u0000\u01ae\u01af\u0001\u0000"+
		"\u0000\u0000\u01af\u01b1\u0001\u0000\u0000\u0000\u01b0\u01a8\u0001\u0000"+
		"\u0000\u0000\u01b0\u01b1\u0001\u0000\u0000\u0000\u01b1\u01b8\u0001\u0000"+
		"\u0000\u0000\u01b2\u01b3\u0005>\u0000\u0000\u01b3\u01b5\u0003\u0018\f"+
		"\u0000\u01b4\u01b6\u0005;\u0000\u0000\u01b5\u01b4\u0001\u0000\u0000\u0000"+
		"\u01b5\u01b6\u0001\u0000\u0000\u0000\u01b6\u01b8\u0001\u0000\u0000\u0000"+
		"\u01b7\u0199\u0001\u0000\u0000\u0000\u01b7\u01b2\u0001\u0000\u0000\u0000"+
		"\u01b7\u01b8\u0001\u0000\u0000\u0000\u01b8\u01ba\u0001\u0000\u0000\u0000"+
		"\u01b9\u0198\u0001\u0000\u0000\u0000\u01b9\u01ba\u0001\u0000\u0000\u0000"+
		"\u01ba\u01da\u0001\u0000\u0000\u0000\u01bb\u01bd\u00058\u0000\u0000\u01bc"+
		"\u01be\u0003\u0018\f\u0000\u01bd\u01bc\u0001\u0000\u0000\u0000\u01bd\u01be"+
		"\u0001\u0000\u0000\u0000\u01be\u01c7\u0001\u0000\u0000\u0000\u01bf\u01c0"+
		"\u0005;\u0000\u0000\u01c0\u01c3\u0003\u0018\f\u0000\u01c1\u01c2\u0005"+
		"?\u0000\u0000\u01c2\u01c4\u0003\u00aeW\u0000\u01c3\u01c1\u0001\u0000\u0000"+
		"\u0000\u01c3\u01c4\u0001\u0000\u0000\u0000\u01c4\u01c6\u0001\u0000\u0000"+
		"\u0000\u01c5\u01bf\u0001\u0000\u0000\u0000\u01c6\u01c9\u0001\u0000\u0000"+
		"\u0000\u01c7\u01c5\u0001\u0000\u0000\u0000\u01c7\u01c8\u0001\u0000\u0000"+
		"\u0000\u01c8\u01d2\u0001\u0000\u0000\u0000\u01c9\u01c7\u0001\u0000\u0000"+
		"\u0000\u01ca\u01d0\u0005;\u0000\u0000\u01cb\u01cc\u0005>\u0000\u0000\u01cc"+
		"\u01ce\u0003\u0018\f\u0000\u01cd\u01cf\u0005;\u0000\u0000\u01ce\u01cd"+
		"\u0001\u0000\u0000\u0000\u01ce\u01cf\u0001\u0000\u0000\u0000\u01cf\u01d1"+
		"\u0001\u0000\u0000\u0000\u01d0\u01cb\u0001\u0000\u0000\u0000\u01d0\u01d1"+
		"\u0001\u0000\u0000\u0000\u01d1\u01d3\u0001\u0000\u0000\u0000\u01d2\u01ca"+
		"\u0001\u0000\u0000\u0000\u01d2\u01d3\u0001\u0000\u0000\u0000\u01d3\u01da"+
		"\u0001\u0000\u0000\u0000\u01d4\u01d5\u0005>\u0000\u0000\u01d5\u01d7\u0003"+
		"\u0018\f\u0000\u01d6\u01d8\u0005;\u0000\u0000\u01d7\u01d6\u0001\u0000"+
		"\u0000\u0000\u01d7\u01d8\u0001\u0000\u0000\u0000\u01d8\u01da\u0001\u0000"+
		"\u0000\u0000\u01d9\u0188\u0001\u0000\u0000\u0000\u01d9\u01bb\u0001\u0000"+
		"\u0000\u0000\u01d9\u01d4\u0001\u0000\u0000\u0000\u01da\u0017\u0001\u0000"+
		"\u0000\u0000\u01db\u01dc\u0003\u00c8d\u0000\u01dc\u0019\u0001\u0000\u0000"+
		"\u0000\u01dd\u01e0\u0003\u001c\u000e\u0000\u01de\u01e0\u0003N\'\u0000"+
		"\u01df\u01dd\u0001\u0000\u0000\u0000\u01df\u01de\u0001\u0000\u0000\u0000"+
		"\u01e0\u001b\u0001\u0000\u0000\u0000\u01e1\u01e6\u0003\u001e\u000f\u0000"+
		"\u01e2\u01e3\u0005=\u0000\u0000\u01e3\u01e5\u0003\u001e\u000f\u0000\u01e4"+
		"\u01e2\u0001\u0000\u0000\u0000\u01e5\u01e8\u0001\u0000\u0000\u0000\u01e6"+
		"\u01e4\u0001\u0000\u0000\u0000\u01e6\u01e7\u0001\u0000\u0000\u0000\u01e7"+
		"\u01ea\u0001\u0000\u0000\u0000\u01e8\u01e6\u0001\u0000\u0000\u0000\u01e9"+
		"\u01eb\u0005=\u0000\u0000\u01ea\u01e9\u0001\u0000\u0000\u0000\u01ea\u01eb"+
		"\u0001\u0000\u0000\u0000\u01eb\u01ec\u0001\u0000\u0000\u0000\u01ec\u01ed"+
		"\u0005,\u0000\u0000\u01ed\u001d\u0001\u0000\u0000\u0000\u01ee\u01f7\u0003"+
		" \u0010\u0000\u01ef\u01f7\u0003(\u0014\u0000\u01f0\u01f7\u0003*\u0015"+
		"\u0000\u01f1\u01f7\u0003,\u0016\u0000\u01f2\u01f7\u00038\u001c\u0000\u01f3"+
		"\u01f7\u0003H$\u0000\u01f4\u01f7\u0003J%\u0000\u01f5\u01f7\u0003L&\u0000"+
		"\u01f6\u01ee\u0001\u0000\u0000\u0000\u01f6\u01ef\u0001\u0000\u0000\u0000"+
		"\u01f6\u01f0\u0001\u0000\u0000\u0000\u01f6\u01f1\u0001\u0000\u0000\u0000"+
		"\u01f6\u01f2\u0001\u0000\u0000\u0000\u01f6\u01f3\u0001\u0000\u0000\u0000"+
		"\u01f6\u01f4\u0001\u0000\u0000\u0000\u01f6\u01f5\u0001\u0000\u0000\u0000"+
		"\u01f7\u001f\u0001\u0000\u0000\u0000\u01f8\u0209\u0003$\u0012\u0000\u01f9"+
		"\u020a\u0003\"\u0011\u0000\u01fa\u01fd\u0003&\u0013\u0000\u01fb\u01fe"+
		"\u0003\u00e8t\u0000\u01fc\u01fe\u0003\u00d6k\u0000\u01fd\u01fb\u0001\u0000"+
		"\u0000\u0000\u01fd\u01fc\u0001\u0000\u0000\u0000\u01fe\u020a\u0001\u0000"+
		"\u0000\u0000\u01ff\u0202\u0005?\u0000\u0000\u0200\u0203\u0003\u00e8t\u0000"+
		"\u0201\u0203\u0003$\u0012\u0000\u0202\u0200\u0001\u0000\u0000\u0000\u0202"+
		"\u0201\u0001\u0000\u0000\u0000\u0203\u0205\u0001\u0000\u0000\u0000\u0204"+
		"\u01ff\u0001\u0000\u0000\u0000\u0205\u0208\u0001\u0000\u0000\u0000\u0206"+
		"\u0204\u0001\u0000\u0000\u0000\u0206\u0207\u0001\u0000\u0000\u0000\u0207"+
		"\u020a\u0001\u0000\u0000\u0000\u0208\u0206\u0001\u0000\u0000\u0000\u0209"+
		"\u01f9\u0001\u0000\u0000\u0000\u0209\u01fa\u0001\u0000\u0000\u0000\u0209"+
		"\u0206\u0001\u0000\u0000\u0000\u020a!\u0001\u0000\u0000\u0000\u020b\u020c"+
		"\u0005<\u0000\u0000\u020c\u020f\u0003\u00aeW\u0000\u020d\u020e\u0005?"+
		"\u0000\u0000\u020e\u0210\u0003\u00aeW\u0000\u020f\u020d\u0001\u0000\u0000"+
		"\u0000\u020f\u0210\u0001\u0000\u0000\u0000\u0210#\u0001\u0000\u0000\u0000"+
		"\u0211\u0214\u0003\u00aeW\u0000\u0212\u0214\u0003\u00c0`\u0000\u0213\u0211"+
		"\u0001\u0000\u0000\u0000\u0213\u0212\u0001\u0000\u0000\u0000\u0214\u021c"+
		"\u0001\u0000\u0000\u0000\u0215\u0218\u0005;\u0000\u0000\u0216\u0219\u0003"+
		"\u00aeW\u0000\u0217\u0219\u0003\u00c0`\u0000\u0218\u0216\u0001\u0000\u0000"+
		"\u0000\u0218\u0217\u0001\u0000\u0000\u0000\u0219\u021b\u0001\u0000\u0000"+
		"\u0000\u021a\u0215\u0001\u0000\u0000\u0000\u021b\u021e\u0001\u0000\u0000"+
		"\u0000\u021c\u021a\u0001\u0000\u0000\u0000\u021c\u021d\u0001\u0000\u0000"+
		"\u0000\u021d\u0220\u0001\u0000\u0000\u0000\u021e\u021c\u0001\u0000\u0000"+
		"\u0000\u021f\u0221\u0005;\u0000\u0000\u0220\u021f\u0001\u0000\u0000\u0000"+
		"\u0220\u0221\u0001\u0000\u0000\u0000\u0221%\u0001\u0000\u0000\u0000\u0222"+
		"\u0223\u0007\u0000\u0000\u0000\u0223\'\u0001\u0000\u0000\u0000\u0224\u0225"+
		"\u0005\u0010\u0000\u0000\u0225\u0226\u0003\u00d4j\u0000\u0226)\u0001\u0000"+
		"\u0000\u0000\u0227\u0228\u0005#\u0000\u0000\u0228+\u0001\u0000\u0000\u0000"+
		"\u0229\u022f\u0003.\u0017\u0000\u022a\u022f\u00030\u0018\u0000\u022b\u022f"+
		"\u00032\u0019\u0000\u022c\u022f\u00036\u001b\u0000\u022d\u022f\u00034"+
		"\u001a\u0000\u022e\u0229\u0001\u0000\u0000\u0000\u022e\u022a\u0001\u0000"+
		"\u0000\u0000\u022e\u022b\u0001\u0000\u0000\u0000\u022e\u022c\u0001\u0000"+
		"\u0000\u0000\u022e\u022d\u0001\u0000\u0000\u0000\u022f-\u0001\u0000\u0000"+
		"\u0000\u0230\u0231\u0005\u000b\u0000\u0000\u0231/\u0001\u0000\u0000\u0000"+
		"\u0232\u0233\u0005\u000e\u0000\u0000\u02331\u0001\u0000\u0000\u0000\u0234"+
		"\u0236\u0005%\u0000\u0000\u0235\u0237\u0003\u00d6k\u0000\u0236\u0235\u0001"+
		"\u0000\u0000\u0000\u0236\u0237\u0001\u0000\u0000\u0000\u02373\u0001\u0000"+
		"\u0000\u0000\u0238\u0239\u0003\u00e8t\u0000\u02395\u0001\u0000\u0000\u0000"+
		"\u023a\u0240\u0005$\u0000\u0000\u023b\u023e\u0003\u00aeW\u0000\u023c\u023d"+
		"\u0005\u0017\u0000\u0000\u023d\u023f\u0003\u00aeW\u0000\u023e\u023c\u0001"+
		"\u0000\u0000\u0000\u023e\u023f\u0001\u0000\u0000\u0000\u023f\u0241\u0001"+
		"\u0000\u0000\u0000\u0240\u023b\u0001\u0000\u0000\u0000\u0240\u0241\u0001"+
		"\u0000\u0000\u0000\u02417\u0001\u0000\u0000\u0000\u0242\u0245\u0003:\u001d"+
		"\u0000\u0243\u0245\u0003<\u001e\u0000\u0244\u0242\u0001\u0000\u0000\u0000"+
		"\u0244\u0243\u0001\u0000\u0000\u0000\u02459\u0001\u0000\u0000\u0000\u0246"+
		"\u0247\u0005\u001a\u0000\u0000\u0247\u0248\u0003D\"\u0000\u0248;\u0001"+
		"\u0000\u0000\u0000\u0249\u0256\u0005\u0017\u0000\u0000\u024a\u024c\u0007"+
		"\u0001\u0000\u0000\u024b\u024a\u0001\u0000\u0000\u0000\u024c\u024f\u0001"+
		"\u0000\u0000\u0000\u024d\u024b\u0001\u0000\u0000\u0000\u024d\u024e\u0001"+
		"\u0000\u0000\u0000\u024e\u0250\u0001\u0000\u0000\u0000\u024f\u024d\u0001"+
		"\u0000\u0000\u0000\u0250\u0257\u0003F#\u0000\u0251\u0253\u0007\u0001\u0000"+
		"\u0000\u0252\u0251\u0001\u0000\u0000\u0000\u0253\u0254\u0001\u0000\u0000"+
		"\u0000\u0254\u0252\u0001\u0000\u0000\u0000\u0254\u0255\u0001\u0000\u0000"+
		"\u0000\u0255\u0257\u0001\u0000\u0000\u0000\u0256\u024d\u0001\u0000\u0000"+
		"\u0000\u0256\u0252\u0001\u0000\u0000\u0000\u0257\u0258\u0001\u0000\u0000"+
		"\u0000\u0258\u025f\u0005\u001a\u0000\u0000\u0259\u0260\u00058\u0000\u0000"+
		"\u025a\u025b\u00059\u0000\u0000\u025b\u025c\u0003B!\u0000\u025c\u025d"+
		"\u0005:\u0000\u0000\u025d\u0260\u0001\u0000\u0000\u0000\u025e\u0260\u0003"+
		"B!\u0000\u025f\u0259\u0001\u0000\u0000\u0000\u025f\u025a\u0001\u0000\u0000"+
		"\u0000\u025f\u025e\u0001\u0000\u0000\u0000\u0260=\u0001\u0000\u0000\u0000"+
		"\u0261\u0264\u0003\u00c8d\u0000\u0262\u0263\u0005\u0007\u0000\u0000\u0263"+
		"\u0265\u0003\u00c8d\u0000\u0264\u0262\u0001\u0000\u0000\u0000\u0264\u0265"+
		"\u0001\u0000\u0000\u0000\u0265?\u0001\u0000\u0000\u0000\u0266\u0269\u0003"+
		"F#\u0000\u0267\u0268\u0005\u0007\u0000\u0000\u0268\u026a\u0003\u00c8d"+
		"\u0000\u0269\u0267\u0001\u0000\u0000\u0000\u0269\u026a\u0001\u0000\u0000"+
		"\u0000\u026aA\u0001\u0000\u0000\u0000\u026b\u0270\u0003>\u001f\u0000\u026c"+
		"\u026d\u0005;\u0000\u0000\u026d\u026f\u0003>\u001f\u0000\u026e\u026c\u0001"+
		"\u0000\u0000\u0000\u026f\u0272\u0001\u0000\u0000\u0000\u0270\u026e\u0001"+
		"\u0000\u0000\u0000\u0270\u0271\u0001\u0000\u0000\u0000\u0271\u0274\u0001"+
		"\u0000\u0000\u0000\u0272\u0270\u0001\u0000\u0000\u0000\u0273\u0275\u0005"+
		";\u0000\u0000\u0274\u0273\u0001\u0000\u0000\u0000\u0274\u0275\u0001\u0000"+
		"\u0000\u0000\u0275C\u0001\u0000\u0000\u0000\u0276\u027b\u0003@ \u0000"+
		"\u0277\u0278\u0005;\u0000\u0000\u0278\u027a\u0003@ \u0000\u0279\u0277"+
		"\u0001\u0000\u0000\u0000\u027a\u027d\u0001\u0000\u0000\u0000\u027b\u0279"+
		"\u0001\u0000\u0000\u0000\u027b\u027c\u0001\u0000\u0000\u0000\u027cE\u0001"+
		"\u0000\u0000\u0000\u027d\u027b\u0001\u0000\u0000\u0000\u027e\u0283\u0003"+
		"\u00c8d\u0000\u027f\u0280\u00056\u0000\u0000\u0280\u0282\u0003\u00c8d"+
		"\u0000\u0281\u027f\u0001\u0000\u0000\u0000\u0282\u0285\u0001\u0000\u0000"+
		"\u0000\u0283\u0281\u0001\u0000\u0000\u0000\u0283\u0284\u0001\u0000\u0000"+
		"\u0000\u0284G\u0001\u0000\u0000\u0000\u0285\u0283\u0001\u0000\u0000\u0000"+
		"\u0286\u0287\u0005\u0018\u0000\u0000\u0287\u028c\u0003\u00c8d\u0000\u0288"+
		"\u0289\u0005;\u0000\u0000\u0289\u028b\u0003\u00c8d\u0000\u028a\u0288\u0001"+
		"\u0000\u0000\u0000\u028b\u028e\u0001\u0000\u0000\u0000\u028c\u028a\u0001"+
		"\u0000\u0000\u0000\u028c\u028d\u0001\u0000\u0000\u0000\u028dI\u0001\u0000"+
		"\u0000\u0000\u028e\u028c\u0001\u0000\u0000\u0000\u028f\u0290\u0005 \u0000"+
		"\u0000\u0290\u0295\u0003\u00c8d\u0000\u0291\u0292\u0005;\u0000\u0000\u0292"+
		"\u0294\u0003\u00c8d\u0000\u0293\u0291\u0001\u0000\u0000\u0000\u0294\u0297"+
		"\u0001\u0000\u0000\u0000\u0295\u0293\u0001\u0000\u0000\u0000\u0295\u0296"+
		"\u0001\u0000\u0000\u0000\u0296K\u0001\u0000\u0000\u0000\u0297\u0295\u0001"+
		"\u0000\u0000\u0000\u0298\u0299\u0005\b\u0000\u0000\u0299\u029c\u0003\u00ae"+
		"W\u0000\u029a\u029b\u0005;\u0000\u0000\u029b\u029d\u0003\u00aeW\u0000"+
		"\u029c\u029a\u0001\u0000\u0000\u0000\u029c\u029d\u0001\u0000\u0000\u0000"+
		"\u029dM\u0001\u0000\u0000\u0000\u029e\u02a9\u0003R)\u0000\u029f\u02a9"+
		"\u0003T*\u0000\u02a0\u02a9\u0003V+\u0000\u02a1\u02a9\u0003X,\u0000\u02a2"+
		"\u02a9\u0003Z-\u0000\u02a3\u02a9\u0003\u000e\u0007\u0000\u02a4\u02a9\u0003"+
		"\u00dam\u0000\u02a5\u02a9\u0003\n\u0005\u0000\u02a6\u02a9\u0003P(\u0000"+
		"\u02a7\u02a9\u0003b1\u0000\u02a8\u029e\u0001\u0000\u0000\u0000\u02a8\u029f"+
		"\u0001\u0000\u0000\u0000\u02a8\u02a0\u0001\u0000\u0000\u0000\u02a8\u02a1"+
		"\u0001\u0000\u0000\u0000\u02a8\u02a2\u0001\u0000\u0000\u0000\u02a8\u02a3"+
		"\u0001\u0000\u0000\u0000\u02a8\u02a4\u0001\u0000\u0000\u0000\u02a8\u02a5"+
		"\u0001\u0000\u0000\u0000\u02a8\u02a6\u0001\u0000\u0000\u0000\u02a8\u02a7"+
		"\u0001\u0000\u0000\u0000\u02a9O\u0001\u0000\u0000\u0000\u02aa\u02ae\u0005"+
		"\t\u0000\u0000\u02ab\u02af\u0003\u000e\u0007\u0000\u02ac\u02af\u0003Z"+
		"-\u0000\u02ad\u02af\u0003V+\u0000\u02ae\u02ab\u0001\u0000\u0000\u0000"+
		"\u02ae\u02ac\u0001\u0000\u0000\u0000\u02ae\u02ad\u0001\u0000\u0000\u0000"+
		"\u02afQ\u0001\u0000\u0000\u0000\u02b0\u02b1\u0005\u0019\u0000\u0000\u02b1"+
		"\u02b2\u0003\u00aeW\u0000\u02b2\u02b3\u0005<\u0000\u0000\u02b3\u02bb\u0003"+
		"`0\u0000\u02b4\u02b5\u0005\u0011\u0000\u0000\u02b5\u02b6\u0003\u00aeW"+
		"\u0000\u02b6\u02b7\u0005<\u0000\u0000\u02b7\u02b8\u0003`0\u0000\u02b8"+
		"\u02ba\u0001\u0000\u0000\u0000\u02b9\u02b4\u0001\u0000\u0000\u0000\u02ba"+
		"\u02bd\u0001\u0000\u0000\u0000\u02bb\u02b9\u0001\u0000\u0000\u0000\u02bb"+
		"\u02bc\u0001\u0000\u0000\u0000\u02bc\u02c1\u0001\u0000\u0000\u0000\u02bd"+
		"\u02bb\u0001\u0000\u0000\u0000\u02be\u02bf\u0005\u0012\u0000\u0000\u02bf"+
		"\u02c0\u0005<\u0000\u0000\u02c0\u02c2\u0003`0\u0000\u02c1\u02be\u0001"+
		"\u0000\u0000\u0000\u02c1\u02c2\u0001\u0000\u0000\u0000\u02c2S\u0001\u0000"+
		"\u0000\u0000\u02c3\u02c4\u0005)\u0000\u0000\u02c4\u02c5\u0003\u00aeW\u0000"+
		"\u02c5\u02c6\u0005<\u0000\u0000\u02c6\u02ca\u0003`0\u0000\u02c7\u02c8"+
		"\u0005\u0012\u0000\u0000\u02c8\u02c9\u0005<\u0000\u0000\u02c9\u02cb\u0003"+
		"`0\u0000\u02ca\u02c7\u0001\u0000\u0000\u0000\u02ca\u02cb\u0001\u0000\u0000"+
		"\u0000\u02cbU\u0001\u0000\u0000\u0000\u02cc\u02cd\u0005\u0016\u0000\u0000"+
		"\u02cd\u02ce\u0003\u00d4j\u0000\u02ce\u02cf\u0005\u001b\u0000\u0000\u02cf"+
		"\u02d0\u0003\u00d6k\u0000\u02d0\u02d1\u0005<\u0000\u0000\u02d1\u02d5\u0003"+
		"`0\u0000\u02d2\u02d3\u0005\u0012\u0000\u0000\u02d3\u02d4\u0005<\u0000"+
		"\u0000\u02d4\u02d6\u0003`0\u0000\u02d5\u02d2\u0001\u0000\u0000\u0000\u02d5"+
		"\u02d6\u0001\u0000\u0000\u0000\u02d6W\u0001\u0000\u0000\u0000\u02d7\u02d8"+
		"\u0005\'\u0000\u0000\u02d8\u02d9\u0005<\u0000\u0000\u02d9\u02ef\u0003"+
		"`0\u0000\u02da\u02db\u0003^/\u0000\u02db\u02dc\u0005<\u0000\u0000\u02dc"+
		"\u02dd\u0003`0\u0000\u02dd\u02df\u0001\u0000\u0000\u0000\u02de\u02da\u0001"+
		"\u0000\u0000\u0000\u02df\u02e0\u0001\u0000\u0000\u0000\u02e0\u02de\u0001"+
		"\u0000\u0000\u0000\u02e0\u02e1\u0001\u0000\u0000\u0000\u02e1\u02e5\u0001"+
		"\u0000\u0000\u0000\u02e2\u02e3\u0005\u0012\u0000\u0000\u02e3\u02e4\u0005"+
		"<\u0000\u0000\u02e4\u02e6\u0003`0\u0000\u02e5\u02e2\u0001\u0000\u0000"+
		"\u0000\u02e5\u02e6\u0001\u0000\u0000\u0000\u02e6\u02ea\u0001\u0000\u0000"+
		"\u0000\u02e7\u02e8\u0005\u0015\u0000\u0000\u02e8\u02e9\u0005<\u0000\u0000"+
		"\u02e9\u02eb\u0003`0\u0000\u02ea\u02e7\u0001\u0000\u0000\u0000\u02ea\u02eb"+
		"\u0001\u0000\u0000\u0000\u02eb\u02f0\u0001\u0000\u0000\u0000\u02ec\u02ed"+
		"\u0005\u0015\u0000\u0000\u02ed\u02ee\u0005<\u0000\u0000\u02ee\u02f0\u0003"+
		"`0\u0000\u02ef\u02de\u0001\u0000\u0000\u0000\u02ef\u02ec\u0001\u0000\u0000"+
		"\u0000\u02f0Y\u0001\u0000\u0000\u0000\u02f1\u02f2\u0005*\u0000\u0000\u02f2"+
		"\u02f7\u0003\\.\u0000\u02f3\u02f4\u0005;\u0000\u0000\u02f4\u02f6\u0003"+
		"\\.\u0000\u02f5\u02f3\u0001\u0000\u0000\u0000\u02f6\u02f9\u0001\u0000"+
		"\u0000\u0000\u02f7\u02f5\u0001\u0000\u0000\u0000\u02f7\u02f8\u0001\u0000"+
		"\u0000\u0000\u02f8\u02fa\u0001\u0000\u0000\u0000\u02f9\u02f7\u0001\u0000"+
		"\u0000\u0000\u02fa\u02fb\u0005<\u0000\u0000\u02fb\u02fc\u0003`0\u0000"+
		"\u02fc[\u0001\u0000\u0000\u0000\u02fd\u0300\u0003\u00aeW\u0000\u02fe\u02ff"+
		"\u0005\u0007\u0000\u0000\u02ff\u0301\u0003\u00c2a\u0000\u0300\u02fe\u0001"+
		"\u0000\u0000\u0000\u0300\u0301\u0001\u0000\u0000\u0000\u0301]\u0001\u0000"+
		"\u0000\u0000\u0302\u0308\u0005\u0013\u0000\u0000\u0303\u0306\u0003\u00ae"+
		"W\u0000\u0304\u0305\u0005\u0007\u0000\u0000\u0305\u0307\u0003\u00c8d\u0000"+
		"\u0306\u0304\u0001\u0000\u0000\u0000\u0306\u0307\u0001\u0000\u0000\u0000"+
		"\u0307\u0309\u0001\u0000\u0000\u0000\u0308\u0303\u0001\u0000\u0000\u0000"+
		"\u0308\u0309\u0001\u0000\u0000\u0000\u0309_\u0001\u0000\u0000\u0000\u030a"+
		"\u0315\u0003\u001c\u000e\u0000\u030b\u030c\u0005,\u0000\u0000\u030c\u030e"+
		"\u0005\u0001\u0000\u0000\u030d\u030f\u0003\u001a\r\u0000\u030e\u030d\u0001"+
		"\u0000\u0000\u0000\u030f\u0310\u0001\u0000\u0000\u0000\u0310\u030e\u0001"+
		"\u0000\u0000\u0000\u0310\u0311\u0001\u0000\u0000\u0000\u0311\u0312\u0001"+
		"\u0000\u0000\u0000\u0312\u0313\u0005\u0002\u0000\u0000\u0313\u0315\u0001"+
		"\u0000\u0000\u0000\u0314\u030a\u0001\u0000\u0000\u0000\u0314\u030b\u0001"+
		"\u0000\u0000\u0000\u0315a\u0001\u0000\u0000\u0000\u0316\u0317\u0005\u001e"+
		"\u0000\u0000\u0317\u0318\u0003d2\u0000\u0318\u0319\u0005<\u0000\u0000"+
		"\u0319\u031a\u0005,\u0000\u0000\u031a\u031c\u0005\u0001\u0000\u0000\u031b"+
		"\u031d\u0003j5\u0000\u031c\u031b\u0001\u0000\u0000\u0000\u031d\u031e\u0001"+
		"\u0000\u0000\u0000\u031e\u031c\u0001\u0000\u0000\u0000\u031e\u031f\u0001"+
		"\u0000\u0000\u0000\u031f\u0320\u0001\u0000\u0000\u0000\u0320\u0321\u0005"+
		"\u0002\u0000\u0000\u0321c\u0001\u0000\u0000\u0000\u0322\u0323\u0003h4"+
		"\u0000\u0323\u0325\u0005;\u0000\u0000\u0324\u0326\u0003f3\u0000\u0325"+
		"\u0324\u0001\u0000\u0000\u0000\u0325\u0326\u0001\u0000\u0000\u0000\u0326"+
		"\u0329\u0001\u0000\u0000\u0000\u0327\u0329\u0003\u00aeW\u0000\u0328\u0322"+
		"\u0001\u0000\u0000\u0000\u0328\u0327\u0001\u0000\u0000\u0000\u0329e\u0001"+
		"\u0000\u0000\u0000\u032a\u032c\u0005;\u0000\u0000\u032b\u032d\u0003h4"+
		"\u0000\u032c\u032b\u0001\u0000\u0000\u0000\u032d\u032e\u0001\u0000\u0000"+
		"\u0000\u032e\u032c\u0001\u0000\u0000\u0000\u032e\u032f\u0001\u0000\u0000"+
		"\u0000\u032f\u0331\u0001\u0000\u0000\u0000\u0330\u0332\u0005;\u0000\u0000"+
		"\u0331\u0330\u0001\u0000\u0000\u0000\u0331\u0332\u0001\u0000\u0000\u0000"+
		"\u0332g\u0001\u0000\u0000\u0000\u0333\u0334\u00058\u0000\u0000\u0334\u0337"+
		"\u0003\u00c2a\u0000\u0335\u0337\u0003\u00aeW\u0000\u0336\u0333\u0001\u0000"+
		"\u0000\u0000\u0336\u0335\u0001\u0000\u0000\u0000\u0337i\u0001\u0000\u0000"+
		"\u0000\u0338\u0339\u0005\f\u0000\u0000\u0339\u033b\u0003n7\u0000\u033a"+
		"\u033c\u0003l6\u0000\u033b\u033a\u0001\u0000\u0000\u0000\u033b\u033c\u0001"+
		"\u0000\u0000\u0000\u033c\u033d\u0001\u0000\u0000\u0000\u033d\u033e\u0005"+
		"<\u0000\u0000\u033e\u033f\u0003`0\u0000\u033fk\u0001\u0000\u0000\u0000"+
		"\u0340\u0341\u0005\u0019\u0000\u0000\u0341\u0342\u0003\u00aeW\u0000\u0342"+
		"m\u0001\u0000\u0000\u0000\u0343\u0346\u0003\u0096K\u0000\u0344\u0346\u0003"+
		"p8\u0000\u0345\u0343\u0001\u0000\u0000\u0000\u0345\u0344\u0001\u0000\u0000"+
		"\u0000\u0346o\u0001\u0000\u0000\u0000\u0347\u034a\u0003r9\u0000\u0348"+
		"\u034a\u0003t:\u0000\u0349\u0347\u0001\u0000\u0000\u0000\u0349\u0348\u0001"+
		"\u0000\u0000\u0000\u034aq\u0001\u0000\u0000\u0000\u034b\u034c\u0003t:"+
		"\u0000\u034c\u034d\u0005\u0007\u0000\u0000\u034d\u034e\u0003\u0088D\u0000"+
		"\u034es\u0001\u0000\u0000\u0000\u034f\u0354\u0003v;\u0000\u0350\u0351"+
		"\u0005B\u0000\u0000\u0351\u0353\u0003v;\u0000\u0352\u0350\u0001\u0000"+
		"\u0000\u0000\u0353\u0356\u0001\u0000\u0000\u0000\u0354\u0352\u0001\u0000"+
		"\u0000\u0000\u0354\u0355\u0001\u0000\u0000\u0000\u0355u\u0001\u0000\u0000"+
		"\u0000\u0356\u0354\u0001\u0000\u0000\u0000\u0357\u0360\u0003x<\u0000\u0358"+
		"\u0360\u0003\u0086C\u0000\u0359\u0360\u0003\u008aE\u0000\u035a\u0360\u0003"+
		"\u008cF\u0000\u035b\u0360\u0003\u0092I\u0000\u035c\u0360\u0003\u0094J"+
		"\u0000\u035d\u0360\u0003\u009eO\u0000\u035e\u0360\u0003\u00a6S\u0000\u035f"+
		"\u0357\u0001\u0000\u0000\u0000\u035f\u0358\u0001\u0000\u0000\u0000\u035f"+
		"\u0359\u0001\u0000\u0000\u0000\u035f\u035a\u0001\u0000\u0000\u0000\u035f"+
		"\u035b\u0001\u0000\u0000\u0000\u035f\u035c\u0001\u0000\u0000\u0000\u035f"+
		"\u035d\u0001\u0000\u0000\u0000\u035f\u035e\u0001\u0000\u0000\u0000\u0360"+
		"w\u0001\u0000\u0000\u0000\u0361\u0362\u0003~?\u0000\u0362\u0363\u0004"+
		"<\u0000\u0000\u0363\u036a\u0001\u0000\u0000\u0000\u0364\u036a\u0003|>"+
		"\u0000\u0365\u036a\u0003\u00ecv\u0000\u0366\u036a\u0005\u001f\u0000\u0000"+
		"\u0367\u036a\u0005&\u0000\u0000\u0368\u036a\u0005\u0014\u0000\u0000\u0369"+
		"\u0361\u0001\u0000\u0000\u0000\u0369\u0364\u0001\u0000\u0000\u0000\u0369"+
		"\u0365\u0001\u0000\u0000\u0000\u0369\u0366\u0001\u0000\u0000\u0000\u0369"+
		"\u0367\u0001\u0000\u0000\u0000\u0369\u0368\u0001\u0000\u0000\u0000\u036a"+
		"y\u0001\u0000\u0000\u0000\u036b\u036c\u0003~?\u0000\u036c\u036d\u0004"+
		"=\u0001\u0000\u036d\u0374\u0001\u0000\u0000\u0000\u036e\u0374\u0003|>"+
		"\u0000\u036f\u0374\u0003\u00ecv\u0000\u0370\u0374\u0005\u001f\u0000\u0000"+
		"\u0371\u0374\u0005&\u0000\u0000\u0372\u0374\u0005\u0014\u0000\u0000\u0373"+
		"\u036b\u0001\u0000\u0000\u0000\u0373\u036e\u0001\u0000\u0000\u0000\u0373"+
		"\u036f\u0001\u0000\u0000\u0000\u0373\u0370\u0001\u0000\u0000\u0000\u0373"+
		"\u0371\u0001\u0000\u0000\u0000\u0373\u0372\u0001\u0000\u0000\u0000\u0374"+
		"{\u0001\u0000\u0000\u0000\u0375\u0376\u0003\u0080@\u0000\u0376\u0377\u0005"+
		"G\u0000\u0000\u0377\u0378\u0003\u0084B\u0000\u0378\u037e\u0001\u0000\u0000"+
		"\u0000\u0379\u037a\u0003\u0080@\u0000\u037a\u037b\u0005H\u0000\u0000\u037b"+
		"\u037c\u0003\u0084B\u0000\u037c\u037e\u0001\u0000\u0000\u0000\u037d\u0375"+
		"\u0001\u0000\u0000\u0000\u037d\u0379\u0001\u0000\u0000\u0000\u037e}\u0001"+
		"\u0000\u0000\u0000\u037f\u0383\u0005\u0004\u0000\u0000\u0380\u0381\u0005"+
		"H\u0000\u0000\u0381\u0383\u0005\u0004\u0000\u0000\u0382\u037f\u0001\u0000"+
		"\u0000\u0000\u0382\u0380\u0001\u0000\u0000\u0000\u0383\u007f\u0001\u0000"+
		"\u0000\u0000\u0384\u0388\u0003\u0082A\u0000\u0385\u0386\u0005H\u0000\u0000"+
		"\u0386\u0388\u0003\u0082A\u0000\u0387\u0384\u0001\u0000\u0000\u0000\u0387"+
		"\u0385\u0001\u0000\u0000\u0000\u0388\u0081\u0001\u0000\u0000\u0000\u0389"+
		"\u038a\u0005\u0004\u0000\u0000\u038a\u0083\u0001\u0000\u0000\u0000\u038b"+
		"\u038c\u0005\u0004\u0000\u0000\u038c\u0085\u0001\u0000\u0000\u0000\u038d"+
		"\u038e\u0003\u0088D\u0000\u038e\u0087\u0001\u0000\u0000\u0000\u038f\u0390"+
		"\u0003\u00c8d\u0000\u0390\u0391\u0004D\u0002\u0000\u0391\u0089\u0001\u0000"+
		"\u0000\u0000\u0392\u0393\u0005(\u0000\u0000\u0393\u008b\u0001\u0000\u0000"+
		"\u0000\u0394\u0395\u0003\u008eG\u0000\u0395\u0396\u0004F\u0003\u0000\u0396"+
		"\u008d\u0001\u0000\u0000\u0000\u0397\u039a\u0003\u00c8d\u0000\u0398\u0399"+
		"\u00056\u0000\u0000\u0399\u039b\u0003\u00c8d\u0000\u039a\u0398\u0001\u0000"+
		"\u0000\u0000\u039b\u039c\u0001\u0000\u0000\u0000\u039c\u039a\u0001\u0000"+
		"\u0000\u0000\u039c\u039d\u0001\u0000\u0000\u0000\u039d\u008f\u0001\u0000"+
		"\u0000\u0000\u039e\u03a1\u0003\u008eG\u0000\u039f\u03a1\u0003\u00c8d\u0000"+
		"\u03a0\u039e\u0001\u0000\u0000\u0000\u03a0\u039f\u0001\u0000\u0000\u0000"+
		"\u03a1\u0091\u0001\u0000\u0000\u0000\u03a2\u03a3\u00059\u0000\u0000\u03a3"+
		"\u03a4\u0003p8\u0000\u03a4\u03a5\u0005:\u0000\u0000\u03a5\u0093\u0001"+
		"\u0000\u0000\u0000\u03a6\u03a8\u0005@\u0000\u0000\u03a7\u03a9\u0003\u0098"+
		"L\u0000\u03a8\u03a7\u0001\u0000\u0000\u0000\u03a8\u03a9\u0001\u0000\u0000"+
		"\u0000\u03a9\u03aa\u0001\u0000\u0000\u0000\u03aa\u03b1\u0005A\u0000\u0000"+
		"\u03ab\u03ad\u00059\u0000\u0000\u03ac\u03ae\u0003\u0096K\u0000\u03ad\u03ac"+
		"\u0001\u0000\u0000\u0000\u03ad\u03ae\u0001\u0000\u0000\u0000\u03ae\u03af"+
		"\u0001\u0000\u0000\u0000\u03af\u03b1\u0005:\u0000\u0000\u03b0\u03a6\u0001"+
		"\u0000\u0000\u0000\u03b0\u03ab\u0001\u0000\u0000\u0000\u03b1\u0095\u0001"+
		"\u0000\u0000\u0000\u03b2\u03b3\u0003\u009aM\u0000\u03b3\u03b5\u0005;\u0000"+
		"\u0000\u03b4\u03b6\u0003\u0098L\u0000\u03b5\u03b4\u0001\u0000\u0000\u0000"+
		"\u03b5\u03b6\u0001\u0000\u0000\u0000\u03b6\u0097\u0001\u0000\u0000\u0000"+
		"\u03b7\u03bc\u0003\u009aM\u0000\u03b8\u03b9\u0005;\u0000\u0000\u03b9\u03bb"+
		"\u0003\u009aM\u0000\u03ba\u03b8\u0001\u0000\u0000\u0000\u03bb\u03be\u0001"+
		"\u0000\u0000\u0000\u03bc\u03ba\u0001\u0000\u0000\u0000\u03bc\u03bd\u0001"+
		"\u0000\u0000\u0000\u03bd\u03c0\u0001\u0000\u0000\u0000\u03be\u03bc\u0001"+
		"\u0000\u0000\u0000\u03bf\u03c1\u0005;\u0000\u0000\u03c0\u03bf\u0001\u0000"+
		"\u0000\u0000\u03c0\u03c1\u0001\u0000\u0000\u0000\u03c1\u0099\u0001\u0000"+
		"\u0000\u0000\u03c2\u03c5\u0003\u009cN\u0000\u03c3\u03c5\u0003p8\u0000"+
		"\u03c4\u03c2\u0001\u0000\u0000\u0000\u03c4\u03c3\u0001\u0000\u0000\u0000"+
		"\u03c5\u009b\u0001\u0000\u0000\u0000\u03c6\u03c7\u00058\u0000\u0000\u03c7"+
		"\u03cb\u0003\u0088D\u0000\u03c8\u03c9\u00058\u0000\u0000\u03c9\u03cb\u0003"+
		"\u008aE\u0000\u03ca\u03c6\u0001\u0000\u0000\u0000\u03ca\u03c8\u0001\u0000"+
		"\u0000\u0000\u03cb\u009d\u0001\u0000\u0000\u0000\u03cc\u03cd\u0005M\u0000"+
		"\u0000\u03cd\u03e6\u0005N\u0000\u0000\u03ce\u03cf\u0005M\u0000\u0000\u03cf"+
		"\u03d1\u0003\u00a4R\u0000\u03d0\u03d2\u0005;\u0000\u0000\u03d1\u03d0\u0001"+
		"\u0000\u0000\u0000\u03d1\u03d2\u0001\u0000\u0000\u0000\u03d2\u03d3\u0001"+
		"\u0000\u0000\u0000\u03d3\u03d4\u0005N\u0000\u0000\u03d4\u03e6\u0001\u0000"+
		"\u0000\u0000\u03d5\u03d6\u0005M\u0000\u0000\u03d6\u03d7\u0003\u00a0P\u0000"+
		"\u03d7\u03d8\u0005;\u0000\u0000\u03d8\u03da\u0003\u00a4R\u0000\u03d9\u03db"+
		"\u0005;\u0000\u0000\u03da\u03d9\u0001\u0000\u0000\u0000\u03da\u03db\u0001"+
		"\u0000\u0000\u0000\u03db\u03dc\u0001\u0000\u0000\u0000\u03dc\u03dd\u0005"+
		"N\u0000\u0000\u03dd\u03e6\u0001\u0000\u0000\u0000\u03de\u03df\u0005M\u0000"+
		"\u0000\u03df\u03e1\u0003\u00a0P\u0000\u03e0\u03e2\u0005;\u0000\u0000\u03e1"+
		"\u03e0\u0001\u0000\u0000\u0000\u03e1\u03e2\u0001\u0000\u0000\u0000\u03e2"+
		"\u03e3\u0001\u0000\u0000\u0000\u03e3\u03e4\u0005N\u0000\u0000\u03e4\u03e6"+
		"\u0001\u0000\u0000\u0000\u03e5\u03cc\u0001\u0000\u0000\u0000\u03e5\u03ce"+
		"\u0001\u0000\u0000\u0000\u03e5\u03d5\u0001\u0000\u0000\u0000\u03e5\u03de"+
		"\u0001\u0000\u0000\u0000\u03e6\u009f\u0001\u0000\u0000\u0000\u03e7\u03ec"+
		"\u0003\u00a2Q\u0000\u03e8\u03e9\u0005;\u0000\u0000\u03e9\u03eb\u0003\u00a2"+
		"Q\u0000\u03ea\u03e8\u0001\u0000\u0000\u0000\u03eb\u03ee\u0001\u0000\u0000"+
		"\u0000\u03ec\u03ea\u0001\u0000\u0000\u0000\u03ec\u03ed\u0001\u0000\u0000"+
		"\u0000\u03ed\u00a1\u0001\u0000\u0000\u0000\u03ee\u03ec\u0001\u0000\u0000"+
		"\u0000\u03ef\u03f2\u0003z=\u0000\u03f0\u03f2\u0003\u008eG\u0000\u03f1"+
		"\u03ef\u0001\u0000\u0000\u0000\u03f1\u03f0\u0001\u0000\u0000\u0000\u03f2"+
		"\u03f3\u0001\u0000\u0000\u0000\u03f3\u03f4\u0005<\u0000\u0000\u03f4\u03f5"+
		"\u0003p8\u0000\u03f5\u00a3\u0001\u0000\u0000\u0000\u03f6\u03f7\u0005>"+
		"\u0000\u0000\u03f7\u03f8\u0003\u0088D\u0000\u03f8\u00a5\u0001\u0000\u0000"+
		"\u0000\u03f9\u03fa\u0003\u0090H\u0000\u03fa\u03fb\u00059\u0000\u0000\u03fb"+
		"\u03fc\u0005:\u0000\u0000\u03fc\u0418\u0001\u0000\u0000\u0000\u03fd\u03fe"+
		"\u0003\u0090H\u0000\u03fe\u03ff\u00059\u0000\u0000\u03ff\u0401\u0003\u00a8"+
		"T\u0000\u0400\u0402\u0005;\u0000\u0000\u0401\u0400\u0001\u0000\u0000\u0000"+
		"\u0401\u0402\u0001\u0000\u0000\u0000\u0402\u0403\u0001\u0000\u0000\u0000"+
		"\u0403\u0404\u0005:\u0000\u0000\u0404\u0418\u0001\u0000\u0000\u0000\u0405"+
		"\u0406\u0003\u0090H\u0000\u0406\u0407\u00059\u0000\u0000\u0407\u0409\u0003"+
		"\u00aaU\u0000\u0408\u040a\u0005;\u0000\u0000\u0409\u0408\u0001\u0000\u0000"+
		"\u0000\u0409\u040a\u0001\u0000\u0000\u0000\u040a\u040b\u0001\u0000\u0000"+
		"\u0000\u040b\u040c\u0005:\u0000\u0000\u040c\u0418\u0001\u0000\u0000\u0000"+
		"\u040d\u040e\u0003\u0090H\u0000\u040e\u040f\u00059\u0000\u0000\u040f\u0410"+
		"\u0003\u00a8T\u0000\u0410\u0411\u0005;\u0000\u0000\u0411\u0413\u0003\u00aa"+
		"U\u0000\u0412\u0414\u0005;\u0000\u0000\u0413\u0412\u0001\u0000\u0000\u0000"+
		"\u0413\u0414\u0001\u0000\u0000\u0000\u0414\u0415\u0001\u0000\u0000\u0000"+
		"\u0415\u0416\u0005:\u0000\u0000\u0416\u0418\u0001\u0000\u0000\u0000\u0417"+
		"\u03f9\u0001\u0000\u0000\u0000\u0417\u03fd\u0001\u0000\u0000\u0000\u0417"+
		"\u0405\u0001\u0000\u0000\u0000\u0417\u040d\u0001\u0000\u0000\u0000\u0418"+
		"\u00a7\u0001\u0000\u0000\u0000\u0419\u041e\u0003p8\u0000\u041a\u041b\u0005"+
		";\u0000\u0000\u041b\u041d\u0003p8\u0000\u041c\u041a\u0001\u0000\u0000"+
		"\u0000\u041d\u0420\u0001\u0000\u0000\u0000\u041e\u041c\u0001\u0000\u0000"+
		"\u0000\u041e\u041f\u0001\u0000\u0000\u0000\u041f\u00a9\u0001\u0000\u0000"+
		"\u0000\u0420\u041e\u0001\u0000\u0000\u0000\u0421\u0426\u0003\u00acV\u0000"+
		"\u0422\u0423\u0005;\u0000\u0000\u0423\u0425\u0003\u00acV\u0000\u0424\u0422"+
		"\u0001\u0000\u0000\u0000\u0425\u0428\u0001\u0000\u0000\u0000\u0426\u0424"+
		"\u0001\u0000\u0000\u0000\u0426\u0427\u0001\u0000\u0000\u0000\u0427\u00ab"+
		"\u0001\u0000\u0000\u0000\u0428\u0426\u0001\u0000\u0000\u0000\u0429\u042a"+
		"\u0003\u00c8d\u0000\u042a\u042b\u0005?\u0000\u0000\u042b\u042c\u0003p"+
		"8\u0000\u042c\u00ad\u0001\u0000\u0000\u0000\u042d\u0433\u0003\u00b6[\u0000"+
		"\u042e\u042f\u0005\u0019\u0000\u0000\u042f\u0430\u0003\u00b6[\u0000\u0430"+
		"\u0431\u0005\u0012\u0000\u0000\u0431\u0432\u0003\u00aeW\u0000\u0432\u0434"+
		"\u0001\u0000\u0000\u0000\u0433\u042e\u0001\u0000\u0000\u0000\u0433\u0434"+
		"\u0001\u0000\u0000\u0000\u0434\u0437\u0001\u0000\u0000\u0000\u0435\u0437"+
		"\u0003\u00b2Y\u0000\u0436\u042d\u0001\u0000\u0000\u0000\u0436\u0435\u0001"+
		"\u0000\u0000\u0000\u0437\u00af\u0001\u0000\u0000\u0000\u0438\u043b\u0003"+
		"\u00b6[\u0000\u0439\u043b\u0003\u00b4Z\u0000\u043a\u0438\u0001\u0000\u0000"+
		"\u0000\u043a\u0439\u0001\u0000\u0000\u0000\u043b\u00b1\u0001\u0000\u0000"+
		"\u0000\u043c\u043e\u0005\u001d\u0000\u0000\u043d\u043f\u0003\u0016\u000b"+
		"\u0000\u043e\u043d\u0001\u0000\u0000\u0000\u043e\u043f\u0001\u0000\u0000"+
		"\u0000\u043f\u0440\u0001\u0000\u0000\u0000\u0440\u0441\u0005<\u0000\u0000"+
		"\u0441\u0442\u0003\u00aeW\u0000\u0442\u00b3\u0001\u0000\u0000\u0000\u0443"+
		"\u0445\u0005\u001d\u0000\u0000\u0444\u0446\u0003\u0016\u000b\u0000\u0445"+
		"\u0444\u0001\u0000\u0000\u0000\u0445\u0446\u0001\u0000\u0000\u0000\u0446"+
		"\u0447\u0001\u0000\u0000\u0000\u0447\u0448\u0005<\u0000\u0000\u0448\u0449"+
		"\u0003\u00b0X\u0000\u0449\u00b5\u0001\u0000\u0000\u0000\u044a\u044f\u0003"+
		"\u00b8\\\u0000\u044b\u044c\u0005\"\u0000\u0000\u044c\u044e\u0003\u00b8"+
		"\\\u0000\u044d\u044b\u0001\u0000\u0000\u0000\u044e\u0451\u0001\u0000\u0000"+
		"\u0000\u044f\u044d\u0001\u0000\u0000\u0000\u044f\u0450\u0001\u0000\u0000"+
		"\u0000\u0450\u00b7\u0001\u0000\u0000\u0000\u0451\u044f\u0001\u0000\u0000"+
		"\u0000\u0452\u0457\u0003\u00ba]\u0000\u0453\u0454\u0005\u0006\u0000\u0000"+
		"\u0454\u0456\u0003\u00ba]\u0000\u0455\u0453\u0001\u0000\u0000\u0000\u0456"+
		"\u0459\u0001\u0000\u0000\u0000\u0457\u0455\u0001\u0000\u0000\u0000\u0457"+
		"\u0458\u0001\u0000\u0000\u0000\u0458\u00b9\u0001\u0000\u0000\u0000\u0459"+
		"\u0457\u0001\u0000\u0000\u0000\u045a\u045b\u0005!\u0000\u0000\u045b\u045e"+
		"\u0003\u00ba]\u0000\u045c\u045e\u0003\u00bc^\u0000\u045d\u045a\u0001\u0000"+
		"\u0000\u0000\u045d\u045c\u0001\u0000\u0000\u0000\u045e\u00bb\u0001\u0000"+
		"\u0000\u0000\u045f\u0465\u0003\u00c2a\u0000\u0460\u0461\u0003\u00be_\u0000"+
		"\u0461\u0462\u0003\u00c2a\u0000\u0462\u0464\u0001\u0000\u0000\u0000\u0463"+
		"\u0460\u0001\u0000\u0000\u0000\u0464\u0467\u0001\u0000\u0000\u0000\u0465"+
		"\u0463\u0001\u0000\u0000\u0000\u0465\u0466\u0001\u0000\u0000\u0000\u0466"+
		"\u00bd\u0001\u0000\u0000\u0000\u0467\u0465\u0001\u0000\u0000\u0000\u0468"+
		"\u0476\u0005O\u0000\u0000\u0469\u0476\u0005P\u0000\u0000\u046a\u0476\u0005"+
		"Q\u0000\u0000\u046b\u0476\u0005R\u0000\u0000\u046c\u0476\u0005S\u0000"+
		"\u0000\u046d\u0476\u0005T\u0000\u0000\u046e\u0476\u0005U\u0000\u0000\u046f"+
		"\u0476\u0005\u001b\u0000\u0000\u0470\u0471\u0005!\u0000\u0000\u0471\u0476"+
		"\u0005\u001b\u0000\u0000\u0472\u0476\u0005\u001c\u0000\u0000\u0473\u0474"+
		"\u0005\u001c\u0000\u0000\u0474\u0476\u0005!\u0000\u0000\u0475\u0468\u0001"+
		"\u0000\u0000\u0000\u0475\u0469\u0001\u0000\u0000\u0000\u0475\u046a\u0001"+
		"\u0000\u0000\u0000\u0475\u046b\u0001\u0000\u0000\u0000\u0475\u046c\u0001"+
		"\u0000\u0000\u0000\u0475\u046d\u0001\u0000\u0000\u0000\u0475\u046e\u0001"+
		"\u0000\u0000\u0000\u0475\u046f\u0001\u0000\u0000\u0000\u0475\u0470\u0001"+
		"\u0000\u0000\u0000\u0475\u0472\u0001\u0000\u0000\u0000\u0475\u0473\u0001"+
		"\u0000\u0000\u0000\u0476\u00bf\u0001\u0000\u0000\u0000\u0477\u0478\u0005"+
		"8\u0000\u0000\u0478\u0479\u0003\u00c2a\u0000\u0479\u00c1\u0001\u0000\u0000"+
		"\u0000\u047a\u047b\u0006a\uffff\uffff\u0000\u047b\u0483\u0003\u00c4b\u0000"+
		"\u047c\u047e\u0007\u0002\u0000\u0000\u047d\u047c\u0001\u0000\u0000\u0000"+
		"\u047e\u047f\u0001\u0000\u0000\u0000\u047f\u047d\u0001\u0000\u0000\u0000"+
		"\u047f\u0480\u0001\u0000\u0000\u0000\u0480\u0481\u0001\u0000\u0000\u0000"+
		"\u0481\u0483\u0003\u00c2a\u0007\u0482\u047a\u0001\u0000\u0000\u0000\u0482"+
		"\u047d\u0001\u0000\u0000\u0000\u0483\u049b\u0001\u0000\u0000\u0000\u0484"+
		"\u0485\n\b\u0000\u0000\u0485\u0486\u0005>\u0000\u0000\u0486\u049a\u0003"+
		"\u00c2a\t\u0487\u0488\n\u0006\u0000\u0000\u0488\u0489\u0007\u0003\u0000"+
		"\u0000\u0489\u049a\u0003\u00c2a\u0007\u048a\u048b\n\u0005\u0000\u0000"+
		"\u048b\u048c\u0007\u0004\u0000\u0000\u048c\u049a\u0003\u00c2a\u0006\u048d"+
		"\u048e\n\u0004\u0000\u0000\u048e\u048f\u0007\u0005\u0000\u0000\u048f\u049a"+
		"\u0003\u00c2a\u0005\u0490\u0491\n\u0003\u0000\u0000\u0491\u0492\u0005"+
		"D\u0000\u0000\u0492\u049a\u0003\u00c2a\u0004\u0493\u0494\n\u0002\u0000"+
		"\u0000\u0494\u0495\u0005C\u0000\u0000\u0495\u049a\u0003\u00c2a\u0003\u0496"+
		"\u0497\n\u0001\u0000\u0000\u0497\u0498\u0005B\u0000\u0000\u0498\u049a"+
		"\u0003\u00c2a\u0002\u0499\u0484\u0001\u0000\u0000\u0000\u0499\u0487\u0001"+
		"\u0000\u0000\u0000\u0499\u048a\u0001\u0000\u0000\u0000\u0499\u048d\u0001"+
		"\u0000\u0000\u0000\u0499\u0490\u0001\u0000\u0000\u0000\u0499\u0493\u0001"+
		"\u0000\u0000\u0000\u0499\u0496\u0001\u0000\u0000\u0000\u049a\u049d\u0001"+
		"\u0000\u0000\u0000\u049b\u0499\u0001\u0000\u0000\u0000\u049b\u049c\u0001"+
		"\u0000\u0000\u0000\u049c\u00c3\u0001\u0000\u0000\u0000\u049d\u049b\u0001"+
		"\u0000\u0000\u0000\u049e\u04a0\u0005\n\u0000\u0000\u049f\u049e\u0001\u0000"+
		"\u0000\u0000\u049f\u04a0\u0001\u0000\u0000\u0000\u04a0\u04a1\u0001\u0000"+
		"\u0000\u0000\u04a1\u04a5\u0003\u00c6c\u0000\u04a2\u04a4\u0003\u00ccf\u0000"+
		"\u04a3\u04a2\u0001\u0000\u0000\u0000\u04a4\u04a7\u0001\u0000\u0000\u0000"+
		"\u04a5\u04a3\u0001\u0000\u0000\u0000\u04a5\u04a6\u0001\u0000\u0000\u0000"+
		"\u04a6\u00c5\u0001\u0000\u0000\u0000\u04a7\u04a5\u0001\u0000\u0000\u0000"+
		"\u04a8\u04ab\u00059\u0000\u0000\u04a9\u04ac\u0003\u00e8t\u0000\u04aa\u04ac"+
		"\u0003\u00cae\u0000\u04ab\u04a9\u0001\u0000\u0000\u0000\u04ab\u04aa\u0001"+
		"\u0000\u0000\u0000\u04ab\u04ac\u0001\u0000\u0000\u0000\u04ac\u04ad\u0001"+
		"\u0000\u0000\u0000\u04ad\u04c4\u0005:\u0000\u0000\u04ae\u04b0\u0005@\u0000"+
		"\u0000\u04af\u04b1\u0003\u00cae\u0000\u04b0\u04af\u0001\u0000\u0000\u0000"+
		"\u04b0\u04b1\u0001\u0000\u0000\u0000\u04b1\u04b2\u0001\u0000\u0000\u0000"+
		"\u04b2\u04c4\u0005A\u0000\u0000\u04b3\u04b5\u0005M\u0000\u0000\u04b4\u04b6"+
		"\u0003\u00d8l\u0000\u04b5\u04b4\u0001\u0000\u0000\u0000\u04b5\u04b6\u0001"+
		"\u0000\u0000\u0000\u04b6\u04b7\u0001\u0000\u0000\u0000\u04b7\u04c4\u0005"+
		"N\u0000\u0000\u04b8\u04c4\u0003\u00c8d\u0000\u04b9\u04c4\u0005\u0004\u0000"+
		"\u0000\u04ba\u04bc\u0005\u0003\u0000\u0000\u04bb\u04ba\u0001\u0000\u0000"+
		"\u0000\u04bc\u04bd\u0001\u0000\u0000\u0000\u04bd\u04bb\u0001\u0000\u0000"+
		"\u0000\u04bd\u04be\u0001\u0000\u0000\u0000\u04be\u04c4\u0001\u0000\u0000"+
		"\u0000\u04bf\u04c4\u00057\u0000\u0000\u04c0\u04c4\u0005\u001f\u0000\u0000"+
		"\u04c1\u04c4\u0005&\u0000\u0000\u04c2\u04c4\u0005\u0014\u0000\u0000\u04c3"+
		"\u04a8\u0001\u0000\u0000\u0000\u04c3\u04ae\u0001\u0000\u0000\u0000\u04c3"+
		"\u04b3\u0001\u0000\u0000\u0000\u04c3\u04b8\u0001\u0000\u0000\u0000\u04c3"+
		"\u04b9\u0001\u0000\u0000\u0000\u04c3\u04bb\u0001\u0000\u0000\u0000\u04c3"+
		"\u04bf\u0001\u0000\u0000\u0000\u04c3\u04c0\u0001\u0000\u0000\u0000\u04c3"+
		"\u04c1\u0001\u0000\u0000\u0000\u04c3\u04c2\u0001\u0000\u0000\u0000\u04c4"+
		"\u00c7\u0001\u0000\u0000\u0000\u04c5\u04c6\u0007\u0006\u0000\u0000\u04c6"+
		"\u00c9\u0001\u0000\u0000\u0000\u04c7\u04ca\u0003\u00aeW\u0000\u04c8\u04ca"+
		"\u0003\u00c0`\u0000\u04c9\u04c7\u0001\u0000\u0000\u0000\u04c9\u04c8\u0001"+
		"\u0000\u0000\u0000\u04ca\u04d9\u0001\u0000\u0000\u0000\u04cb\u04da\u0003"+
		"\u00e2q\u0000\u04cc\u04cf\u0005;\u0000\u0000\u04cd\u04d0\u0003\u00aeW"+
		"\u0000\u04ce\u04d0\u0003\u00c0`\u0000\u04cf\u04cd\u0001\u0000\u0000\u0000"+
		"\u04cf\u04ce\u0001\u0000\u0000\u0000\u04d0\u04d2\u0001\u0000\u0000\u0000"+
		"\u04d1\u04cc\u0001\u0000\u0000\u0000\u04d2\u04d5\u0001\u0000\u0000\u0000"+
		"\u04d3\u04d1\u0001\u0000\u0000\u0000\u04d3\u04d4\u0001\u0000\u0000\u0000"+
		"\u04d4\u04d7\u0001\u0000\u0000\u0000\u04d5\u04d3\u0001\u0000\u0000\u0000"+
		"\u04d6\u04d8\u0005;\u0000\u0000\u04d7\u04d6\u0001\u0000\u0000\u0000\u04d7"+
		"\u04d8\u0001\u0000\u0000\u0000\u04d8\u04da\u0001\u0000\u0000\u0000\u04d9"+
		"\u04cb\u0001\u0000\u0000\u0000\u04d9\u04d3\u0001\u0000\u0000\u0000\u04da"+
		"\u00cb\u0001\u0000\u0000\u0000\u04db\u04dd\u00059\u0000\u0000\u04dc\u04de"+
		"\u0003\u00dcn\u0000\u04dd\u04dc\u0001\u0000\u0000\u0000\u04dd\u04de\u0001"+
		"\u0000\u0000\u0000\u04de\u04df\u0001\u0000\u0000\u0000\u04df\u04e7\u0005"+
		":\u0000\u0000\u04e0\u04e1\u0005@\u0000\u0000\u04e1\u04e2\u0003\u00ceg"+
		"\u0000\u04e2\u04e3\u0005A\u0000\u0000\u04e3\u04e7\u0001\u0000\u0000\u0000"+
		"\u04e4\u04e5\u00056\u0000\u0000\u04e5\u04e7\u0003\u00c8d\u0000\u04e6\u04db"+
		"\u0001\u0000\u0000\u0000\u04e6\u04e0\u0001\u0000\u0000\u0000\u04e6\u04e4"+
		"\u0001\u0000\u0000\u0000\u04e7\u00cd\u0001\u0000\u0000\u0000\u04e8\u04ed"+
		"\u0003\u00d0h\u0000\u04e9\u04ea\u0005;\u0000\u0000\u04ea\u04ec\u0003\u00d0"+
		"h\u0000\u04eb\u04e9\u0001\u0000\u0000\u0000\u04ec\u04ef\u0001\u0000\u0000"+
		"\u0000\u04ed\u04eb\u0001\u0000\u0000\u0000\u04ed\u04ee\u0001\u0000\u0000"+
		"\u0000\u04ee\u04f1\u0001\u0000\u0000\u0000\u04ef\u04ed\u0001\u0000\u0000"+
		"\u0000\u04f0\u04f2\u0005;\u0000\u0000\u04f1\u04f0\u0001\u0000\u0000\u0000"+
		"\u04f1\u04f2\u0001\u0000\u0000\u0000\u04f2\u00cf\u0001\u0000\u0000\u0000"+
		"\u04f3\u04ff\u0003\u00aeW\u0000\u04f4\u04f6\u0003\u00aeW\u0000\u04f5\u04f4"+
		"\u0001\u0000\u0000\u0000\u04f5\u04f6\u0001\u0000\u0000\u0000\u04f6\u04f7"+
		"\u0001\u0000\u0000\u0000\u04f7\u04f9\u0005<\u0000\u0000\u04f8\u04fa\u0003"+
		"\u00aeW\u0000\u04f9\u04f8\u0001\u0000\u0000\u0000\u04f9\u04fa\u0001\u0000"+
		"\u0000\u0000\u04fa\u04fc\u0001\u0000\u0000\u0000\u04fb\u04fd\u0003\u00d2"+
		"i\u0000\u04fc\u04fb\u0001\u0000\u0000\u0000\u04fc\u04fd\u0001\u0000\u0000"+
		"\u0000\u04fd\u04ff\u0001\u0000\u0000\u0000\u04fe\u04f3\u0001\u0000\u0000"+
		"\u0000\u04fe\u04f5\u0001\u0000\u0000\u0000\u04ff\u00d1\u0001\u0000\u0000"+
		"\u0000\u0500\u0502\u0005<\u0000\u0000\u0501\u0503\u0003\u00aeW\u0000\u0502"+
		"\u0501\u0001\u0000\u0000\u0000\u0502\u0503\u0001\u0000\u0000\u0000\u0503"+
		"\u00d3\u0001\u0000\u0000\u0000\u0504\u0507\u0003\u00c2a\u0000\u0505\u0507"+
		"\u0003\u00c0`\u0000\u0506\u0504\u0001\u0000\u0000\u0000\u0506\u0505\u0001"+
		"\u0000\u0000\u0000\u0507\u050f\u0001\u0000\u0000\u0000\u0508\u050b\u0005"+
		";\u0000\u0000\u0509\u050c\u0003\u00c2a\u0000\u050a\u050c\u0003\u00c0`"+
		"\u0000\u050b\u0509\u0001\u0000\u0000\u0000\u050b\u050a\u0001\u0000\u0000"+
		"\u0000\u050c\u050e\u0001\u0000\u0000\u0000\u050d\u0508\u0001\u0000\u0000"+
		"\u0000\u050e\u0511\u0001\u0000\u0000\u0000\u050f\u050d\u0001\u0000\u0000"+
		"\u0000\u050f\u0510\u0001\u0000\u0000\u0000\u0510\u0513\u0001\u0000\u0000"+
		"\u0000\u0511\u050f\u0001\u0000\u0000\u0000\u0512\u0514\u0005;\u0000\u0000"+
		"\u0513\u0512\u0001\u0000\u0000\u0000\u0513\u0514\u0001\u0000\u0000\u0000"+
		"\u0514\u00d5\u0001\u0000\u0000\u0000\u0515\u051a\u0003\u00aeW\u0000\u0516"+
		"\u0517\u0005;\u0000\u0000\u0517\u0519\u0003\u00aeW\u0000\u0518\u0516\u0001"+
		"\u0000\u0000\u0000\u0519\u051c\u0001\u0000\u0000\u0000\u051a\u0518\u0001"+
		"\u0000\u0000\u0000\u051a\u051b\u0001\u0000\u0000\u0000\u051b\u051e\u0001"+
		"\u0000\u0000\u0000\u051c\u051a\u0001\u0000\u0000\u0000\u051d\u051f\u0005"+
		";\u0000\u0000\u051e\u051d\u0001\u0000\u0000\u0000\u051e\u051f\u0001\u0000"+
		"\u0000\u0000\u051f\u00d7\u0001\u0000\u0000\u0000\u0520\u0521\u0003\u00ae"+
		"W\u0000\u0521\u0522\u0005<\u0000\u0000\u0522\u0523\u0003\u00aeW\u0000"+
		"\u0523\u0527\u0001\u0000\u0000\u0000\u0524\u0525\u0005>\u0000\u0000\u0525"+
		"\u0527\u0003\u00c2a\u0000\u0526\u0520\u0001\u0000\u0000\u0000\u0526\u0524"+
		"\u0001\u0000\u0000\u0000\u0527\u053a\u0001\u0000\u0000\u0000\u0528\u053b"+
		"\u0003\u00e2q\u0000\u0529\u0530\u0005;\u0000\u0000\u052a\u052b\u0003\u00ae"+
		"W\u0000\u052b\u052c\u0005<\u0000\u0000\u052c\u052d\u0003\u00aeW\u0000"+
		"\u052d\u0531\u0001\u0000\u0000\u0000\u052e\u052f\u0005>\u0000\u0000\u052f"+
		"\u0531\u0003\u00c2a\u0000\u0530\u052a\u0001\u0000\u0000\u0000\u0530\u052e"+
		"\u0001\u0000\u0000\u0000\u0531\u0533\u0001\u0000\u0000\u0000\u0532\u0529"+
		"\u0001\u0000\u0000\u0000\u0533\u0536\u0001\u0000\u0000\u0000\u0534\u0532"+
		"\u0001\u0000\u0000\u0000\u0534\u0535\u0001\u0000\u0000\u0000\u0535\u0538"+
		"\u0001\u0000\u0000\u0000\u0536\u0534\u0001\u0000\u0000\u0000\u0537\u0539"+
		"\u0005;\u0000\u0000\u0538\u0537\u0001\u0000\u0000\u0000\u0538\u0539\u0001"+
		"\u0000\u0000\u0000\u0539\u053b\u0001\u0000\u0000\u0000\u053a\u0528\u0001"+
		"\u0000\u0000\u0000\u053a\u0534\u0001\u0000\u0000\u0000\u053b\u0551\u0001"+
		"\u0000\u0000\u0000\u053c\u053f\u0003\u00aeW\u0000\u053d\u053f\u0003\u00c0"+
		"`\u0000\u053e\u053c\u0001\u0000\u0000\u0000\u053e\u053d\u0001\u0000\u0000"+
		"\u0000\u053f\u054e\u0001\u0000\u0000\u0000\u0540\u054f\u0003\u00e2q\u0000"+
		"\u0541\u0544\u0005;\u0000\u0000\u0542\u0545\u0003\u00aeW\u0000\u0543\u0545"+
		"\u0003\u00c0`\u0000\u0544\u0542\u0001\u0000\u0000\u0000\u0544\u0543\u0001"+
		"\u0000\u0000\u0000\u0545\u0547\u0001\u0000\u0000\u0000\u0546\u0541\u0001"+
		"\u0000\u0000\u0000\u0547\u054a\u0001\u0000\u0000\u0000\u0548\u0546\u0001"+
		"\u0000\u0000\u0000\u0548\u0549\u0001\u0000\u0000\u0000\u0549\u054c\u0001"+
		"\u0000\u0000\u0000\u054a\u0548\u0001\u0000\u0000\u0000\u054b\u054d\u0005"+
		";\u0000\u0000\u054c\u054b\u0001\u0000\u0000\u0000\u054c\u054d\u0001\u0000"+
		"\u0000\u0000\u054d\u054f\u0001\u0000\u0000\u0000\u054e\u0540\u0001\u0000"+
		"\u0000\u0000\u054e\u0548\u0001\u0000\u0000\u0000\u054f\u0551\u0001\u0000"+
		"\u0000\u0000\u0550\u0526\u0001\u0000\u0000\u0000\u0550\u053e\u0001\u0000"+
		"\u0000\u0000\u0551\u00d9\u0001\u0000\u0000\u0000\u0552\u0553\u0005\r\u0000"+
		"\u0000\u0553\u0559\u0003\u00c8d\u0000\u0554\u0556\u00059\u0000\u0000\u0555"+
		"\u0557\u0003\u00dcn\u0000\u0556\u0555\u0001\u0000\u0000\u0000\u0556\u0557"+
		"\u0001\u0000\u0000\u0000\u0557\u0558\u0001\u0000\u0000\u0000\u0558\u055a"+
		"\u0005:\u0000\u0000\u0559\u0554\u0001\u0000\u0000\u0000\u0559\u055a\u0001"+
		"\u0000\u0000\u0000\u055a\u055b\u0001\u0000\u0000\u0000\u055b\u055c\u0005"+
		"<\u0000\u0000\u055c\u055d\u0003`0\u0000\u055d\u00db\u0001\u0000\u0000"+
		"\u0000\u055e\u0563\u0003\u00deo\u0000\u055f\u0560\u0005;\u0000\u0000\u0560"+
		"\u0562\u0003\u00deo\u0000\u0561\u055f\u0001\u0000\u0000\u0000\u0562\u0565"+
		"\u0001\u0000\u0000\u0000\u0563\u0561\u0001\u0000\u0000\u0000\u0563\u0564"+
		"\u0001\u0000\u0000\u0000\u0564\u0567\u0001\u0000\u0000\u0000\u0565\u0563"+
		"\u0001\u0000\u0000\u0000\u0566\u0568\u0005;\u0000\u0000\u0567\u0566\u0001"+
		"\u0000\u0000\u0000\u0567\u0568\u0001\u0000\u0000\u0000\u0568\u00dd\u0001"+
		"\u0000\u0000\u0000\u0569\u056b\u0003\u00aeW\u0000\u056a\u056c\u0003\u00e2"+
		"q\u0000\u056b\u056a\u0001\u0000\u0000\u0000\u056b\u056c\u0001\u0000\u0000"+
		"\u0000\u056c\u0576\u0001\u0000\u0000\u0000\u056d\u056e\u0003\u00aeW\u0000"+
		"\u056e\u056f\u0005?\u0000\u0000\u056f\u0570\u0003\u00aeW\u0000\u0570\u0576"+
		"\u0001\u0000\u0000\u0000\u0571\u0572\u0005>\u0000\u0000\u0572\u0576\u0003"+
		"\u00aeW\u0000\u0573\u0574\u00058\u0000\u0000\u0574\u0576\u0003\u00aeW"+
		"\u0000\u0575\u0569\u0001\u0000\u0000\u0000\u0575\u056d\u0001\u0000\u0000"+
		"\u0000\u0575\u0571\u0001\u0000\u0000\u0000\u0575\u0573\u0001\u0000\u0000"+
		"\u0000\u0576\u00df\u0001\u0000\u0000\u0000\u0577\u057a\u0003\u00e2q\u0000"+
		"\u0578\u057a\u0003\u00e4r\u0000\u0579\u0577\u0001\u0000\u0000\u0000\u0579"+
		"\u0578\u0001\u0000\u0000\u0000\u057a\u00e1\u0001\u0000\u0000\u0000\u057b"+
		"\u057d\u0005\t\u0000\u0000\u057c\u057b\u0001\u0000\u0000\u0000\u057c\u057d"+
		"\u0001\u0000\u0000\u0000\u057d\u057e\u0001\u0000\u0000\u0000\u057e\u057f"+
		"\u0005\u0016\u0000\u0000\u057f\u0580\u0003\u00d4j\u0000\u0580\u0581\u0005"+
		"\u001b\u0000\u0000\u0581\u0583\u0003\u00b6[\u0000\u0582\u0584\u0003\u00e0"+
		"p\u0000\u0583\u0582\u0001\u0000\u0000\u0000\u0583\u0584\u0001\u0000\u0000"+
		"\u0000\u0584\u00e3\u0001\u0000\u0000\u0000\u0585\u0586\u0005\u0019\u0000"+
		"\u0000\u0586\u0588\u0003\u00b0X\u0000\u0587\u0589\u0003\u00e0p\u0000\u0588"+
		"\u0587\u0001\u0000\u0000\u0000\u0588\u0589\u0001\u0000\u0000\u0000\u0589"+
		"\u00e5\u0001\u0000\u0000\u0000\u058a\u058b\u0003\u00c8d\u0000\u058b\u00e7"+
		"\u0001\u0000\u0000\u0000\u058c\u058e\u0005+\u0000\u0000\u058d\u058f\u0003"+
		"\u00eau\u0000\u058e\u058d\u0001\u0000\u0000\u0000\u058e\u058f\u0001\u0000"+
		"\u0000\u0000\u058f\u00e9\u0001\u0000\u0000\u0000\u0590\u0591\u0005\u0017"+
		"\u0000\u0000\u0591\u0594\u0003\u00aeW\u0000\u0592\u0594\u0003\u00d6k\u0000"+
		"\u0593\u0590\u0001\u0000\u0000\u0000\u0593\u0592\u0001\u0000\u0000\u0000"+
		"\u0594\u00eb\u0001\u0000\u0000\u0000\u0595\u0597\u0005\u0003\u0000\u0000"+
		"\u0596\u0595\u0001\u0000\u0000\u0000\u0597\u0598\u0001\u0000\u0000\u0000"+
		"\u0598\u0596\u0001\u0000\u0000\u0000\u0598\u0599\u0001\u0000\u0000\u0000"+
		"\u0599\u00ed\u0001\u0000\u0000\u0000\u00c9\u00f3\u00f7\u00f9\u0102\u010b"+
		"\u010e\u0115\u011b\u0125\u012c\u0133\u0139\u013d\u0143\u0149\u014d\u0154"+
		"\u0156\u0158\u015d\u015f\u0161\u0165\u016b\u016f\u0176\u0178\u017a\u017f"+
		"\u0181\u0186\u018b\u0191\u0195\u019b\u01a1\u01a5\u01ac\u01ae\u01b0\u01b5"+
		"\u01b7\u01b9\u01bd\u01c3\u01c7\u01ce\u01d0\u01d2\u01d7\u01d9\u01df\u01e6"+
		"\u01ea\u01f6\u01fd\u0202\u0206\u0209\u020f\u0213\u0218\u021c\u0220\u022e"+
		"\u0236\u023e\u0240\u0244\u024d\u0254\u0256\u025f\u0264\u0269\u0270\u0274"+
		"\u027b\u0283\u028c\u0295\u029c\u02a8\u02ae\u02bb\u02c1\u02ca\u02d5\u02e0"+
		"\u02e5\u02ea\u02ef\u02f7\u0300\u0306\u0308\u0310\u0314\u031e\u0325\u0328"+
		"\u032e\u0331\u0336\u033b\u0345\u0349\u0354\u035f\u0369\u0373\u037d\u0382"+
		"\u0387\u039c\u03a0\u03a8\u03ad\u03b0\u03b5\u03bc\u03c0\u03c4\u03ca\u03d1"+
		"\u03da\u03e1\u03e5\u03ec\u03f1\u0401\u0409\u0413\u0417\u041e\u0426\u0433"+
		"\u0436\u043a\u043e\u0445\u044f\u0457\u045d\u0465\u0475\u047f\u0482\u0499"+
		"\u049b\u049f\u04a5\u04ab\u04b0\u04b5\u04bd\u04c3\u04c9\u04cf\u04d3\u04d7"+
		"\u04d9\u04dd\u04e6\u04ed\u04f1\u04f5\u04f9\u04fc\u04fe\u0502\u0506\u050b"+
		"\u050f\u0513\u051a\u051e\u0526\u0530\u0534\u0538\u053a\u053e\u0544\u0548"+
		"\u054c\u054e\u0550\u0556\u0559\u0563\u0567\u056b\u0575\u0579\u057c\u0583"+
		"\u0588\u058e\u0593\u0598";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
