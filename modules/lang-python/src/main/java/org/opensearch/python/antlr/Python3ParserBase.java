package org.opensearch.python.antlr;

import org.antlr.v4.runtime.*;

public abstract class Python3ParserBase extends Parser
{
    protected Python3ParserBase(TokenStream input)
    {
	super(input);
    }

    public boolean CannotBePlusMinus()
    {
	return true;
    }

    public boolean CannotBeDotLpEq()
    {
	return true;
    }
}
