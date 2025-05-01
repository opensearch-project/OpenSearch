package org.opensearch.analysis.layered;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.LinkedList;
import java.lang.StringBuilder;
import java.util.Queue;

/**
 * A token filter that splits tokens like "word/POS/FOO" into multiple tokens at the same position.
 */
public class LayeredTokenFilter extends TokenFilter {
    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAttr = addAttribute(PositionIncrementAttribute.class);
    private final Queue<String> buffer = new LinkedList<>();
    private final StringBuilder sb = new StringBuilder();
    private State savedState = null;

    /**
     * Default Constructor
     * @param input input 
     **/
    public LayeredTokenFilter(TokenStream input) {
        super(input);
    }

    /**
     * makeToken make's token for each layer
     * @param part the token for the layer
     * @param layerNo the layer number
     **/
    private String makeToken(String part, int layerNo) {
	sb.setLength(0);
	char prefix = (char)('a' + layerNo);
	sb.append(prefix);
	sb.append('_');
	sb.append(part);
	return sb.toString();
    }

    /**
     * incrementToken
     **/
    @Override
    public boolean incrementToken() throws IOException {
	if (!buffer.isEmpty()) {
	    restoreState(savedState);
	    termAttr.setEmpty().append(buffer.poll());
	    posIncrAttr.setPositionIncrement(0);
	    return true;
	}

	if (!input.incrementToken()) return false;

	String token = termAttr.toString();
	String[] parts = token.split("/");

	if (parts.length > 1) {
	    termAttr.setEmpty().append(makeToken(parts[0], 0));

	    for (int i = 1; i < parts.length; i++) {
		buffer.add(makeToken(parts[i], i));
	    }
	    savedState = captureState();
	} else {
	    // just pass through the single part as-is
	    termAttr.setEmpty().append(makeToken(parts[0], 0));
	}

	return true;
    }
}
