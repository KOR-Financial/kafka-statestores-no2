/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package io.techasylum.kafka.statestore.document.no2;

import org.dizitart.no2.fulltext.BaseTextTokenizer;
import org.dizitart.no2.fulltext.TextTokenizer;
import org.dizitart.no2.fulltext.UniversalTextTokenizer;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * A {@link TextTokenizer} implementation with support for various languages and
 * configurable whitespace characters.
 */
public class FullTextTokenizer extends UniversalTextTokenizer {

	private final String whitespaceChars;

	/**
	 * @param whitespaceChars a String containing all characters which need to be considered as whitespace characters.
	 * @see BaseTextTokenizer
	 */
	public FullTextTokenizer(String whitespaceChars) {
		this.whitespaceChars = whitespaceChars;
	}

	@Override
	public Set<String> tokenize(String text) {
		Set<String> words = new HashSet<>();
		if (text != null) {
			StringTokenizer tokenizer = new StringTokenizer(text, whitespaceChars);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				word = convertWord(word);
				if (word != null) {
					words.add(word);
				}
			}
		}
		return words;
	}

	/**
	 * Converts a `word` into all lower case and checks if it is a known stop word. If it
	 * is, then the `word` will be discarded and will not be considered as a valid token.
	 * @param word the word
	 * @return the tokenized word in all upper case.
	 */
	protected String convertWord(String word) {
		word = word.toLowerCase();
		if (stopWords().contains(word)) {
			return null;
		}
		return word;
	}

}
