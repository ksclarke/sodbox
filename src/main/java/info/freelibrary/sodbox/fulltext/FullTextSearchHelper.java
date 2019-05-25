
package info.freelibrary.sodbox.fulltext;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;

import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Storage;

/**
 * Helper class for full text search. This class provides functionality for parsing and stemming query and tuning
 * document rank calculation
 */
public class FullTextSearchHelper extends Persistent {

    /**
     * Keyword for logical AND in query
     */
    public static final String AND = "AND";

    /**
     * Keyword for logical OR in query
     */
    public static final String OR = "OR";

    /**
     * Keyword for logical NOT in query
     */
    public static final String NOT = "NOT";

    /**
     * List of stop words ignored in parsed text and query
     */
    public static final String[] STOP_WORDS = new String[] { "a", "the", "at", "on", "of", "to", "an" };

    /**
     * Occurrence kind weights array.
     */
    static final float[] OCCURRENCE_KIND_WEIGHTS = new float[0];

    /**
     * Weight of nearness criteria in rank formula
     */
    public float myNearnessWeight = 10.0f;

    /**
     * Penalty of locating search keywords in reverse order
     */
    public int myWordSwapPenalty = 10;

    /**
     * Maximal word length
     */
    public int myMaxWordLength = 100;

    /**
     * A stop list.
     */
    protected transient HashSet myStopList;

    /**
     * Full text search helper constructor
     */
    public FullTextSearchHelper(final Storage aStorage) {
        super(aStorage);
        fillStopList();
    }

    protected FullTextSearchHelper() {
    }

    /**
     * Perform stemming of the word
     *
     * @param aWord word to be stemmed
     * @param aLanguage language of the word (null if unknown)
     * @return normal forms of the word (some words belongs to more than one part of the speech, so there are can be
     *         more than one normal form)
     */
    public String[] getNormalForms(final String aWord, final String aLanguage) {
        return new String[] { aWord };
    }

    /**
     * Check of character is part of the word
     */
    public boolean isWordChar(final char aChar) {
        return Character.isLetter(aChar) || Character.isDigit(aChar);
    }

    /**
     * Split text of the documents into tokens
     *
     * @param aReader stream with document text
     * @return array of occurrences of words in the document
     */
    @SuppressWarnings("unchecked")
    public Occurrence[] parseText(final Reader aReader) throws IOException {
        final ArrayList list = new ArrayList();

        int position = 0;
        int character = aReader.read();

        while (character > 0) {
            if (isWordChar((char) character)) {
                final StringBuffer buf = new StringBuffer();
                final int wordPos = position;

                do {
                    position += 1;
                    buf.append((char) character);
                    character = aReader.read();
                } while (character > 0 && isWordChar((char) character));

                final String word = buf.toString().toLowerCase();

                if (word.length() <= myMaxWordLength && !isStopWord(word)) {
                    list.add(new Occurrence(word, wordPos, 0));
                }
            } else {
                position += 1;
                character = aReader.read();
            }
        }

        return (Occurrence[]) list.toArray(new Occurrence[list.size()]);
    }

    @SuppressWarnings("unchecked")
    protected void fillStopList() {
        myStopList = new HashSet();
        for (int i = 0; i < STOP_WORDS.length; i++) {
            myStopList.add(STOP_WORDS[i]);
        }
    }

    @Override
    public void onLoad() {
        fillStopList();
    }

    /**
     * Check if word is stop word and should bw not included in index
     *
     * @param aWord lower-cased word
     * @return true if word is in stop list, false otherwise
     */
    public boolean isStopWord(final String aWord) {
        return myStopList.contains(aWord);
    }

    protected FullTextQuery disjunction(final QueryScanner aScanner) {
        final FullTextQuery left = conjunction(aScanner);

        if (aScanner.myToken == QueryScanner.TKN_OR) {
            final FullTextQuery right = disjunction(aScanner);

            if (left != null && right != null) {
                return new FullTextQueryBinaryOp(FullTextQuery.OR, left, right);
            } else if (right != null) {
                return right;
            }
        }

        return left;
    }

    protected FullTextQuery conjunction(final QueryScanner aScanner) {
        final FullTextQuery left = term(aScanner);

        if (aScanner.myToken == QueryScanner.TKN_WORD || aScanner.myToken == QueryScanner.TKN_AND) {
            if (aScanner.myToken == QueryScanner.TKN_WORD) {
                aScanner.myUnget = true;
            }

            final int cop = aScanner.myQueryInQuotes ? FullTextQuery.NEAR : FullTextQuery.AND;
            final FullTextQuery right = disjunction(aScanner);

            if (left != null && right != null) {
                return new FullTextQueryBinaryOp(cop, left, right);
            } else if (right != null) {
                return right;
            }
        }

        return left;
    }

    protected FullTextQuery term(final QueryScanner aScanner) {
        FullTextQuery query = null;

        switch (aScanner.scan()) {
            case QueryScanner.TKN_NOT:
                query = term(aScanner);
                return query != null ? new FullTextQueryUnaryOp(FullTextQuery.NOT, query) : null;
            case QueryScanner.TKN_LPAR:
                query = disjunction(aScanner);
                break;
            case QueryScanner.TKN_WORD:
                query = new FullTextQueryMatchOp(aScanner.myQueryInQuotes ? FullTextQuery.STRICT_MATCH
                        : FullTextQuery.MATCH, aScanner.myWord, aScanner.myWordPosition);
                break;
            case QueryScanner.TKN_EOQ:
                return null;
            default:
        }

        aScanner.scan();

        return query;
    }

    /**
     * Parse a query using the supplied language.
     *
     * @param aQuery A query
     * @param aLanguage A language
     * @return A full-text query
     */
    public FullTextQuery parseQuery(final String aQuery, final String aLanguage) {
        return disjunction(new QueryScanner(aQuery, aLanguage));
    }

    /**
     * Get occurrence kind weight. Occurrence kinds can be: in-title, in-header, emphased,... It is up to the document
     * scanner implementation how to enumerate occurence kinds. These is only one limitation - number of difference
     * kinds should not exceed 8.
     *
     * @return array with weights of each occurrence kind
     */
    public float[] getOccurrenceKindWeights() {
        return OCCURRENCE_KIND_WEIGHTS;
    }

    /**
     * Get weight of nearness criteria in document rank. Document rank is calculated as (keywordRank*(1 +
     * nearness*nearnessWeight))
     *
     * @return weight of nearness criteria
     */
    public float getNearnessWeight() {
        return 10.0f;
    }

    /**
     * Get penalty of inverse word order in the text. Assume that document text contains phrase "ah oh ugh". And query
     * "ugh ah" is executed. The distance between "ugh" and "ah" in the document text is 6. But as far as them are in
     * difference order than in query, this distance will be multiplied on "swap penalty", so if swap penalty is 10,
     * then distance between these two word is considered to be 60.
     *
     * @return swap penalty
     */
    public int getWordSwapPenalty() {
        return 10;
    }

    protected class QueryScanner {

        static final int TKN_EOQ = 0;

        static final int TKN_WORD = 1;

        static final int TKN_AND = 2;

        static final int TKN_OR = 3;

        static final int TKN_NOT = 4;

        static final int TKN_LPAR = 5;

        static final int TKN_RPAR = 6;

        String myQuery;

        int myPosition;

        boolean myQueryInQuotes;

        boolean myUnget;

        String myWord;

        int myWordPosition;

        int myToken;

        String myLanguage;

        QueryScanner(final String aQuery, final String aLanguage) {
            myQuery = aQuery;
            myLanguage = aLanguage;
        }

        int scan() {
            if (myUnget) {
                myUnget = false;
                return myToken;
            }

            final int len = myQuery.length();
            int p = myPosition;
            final String q = myQuery;

            while (p < len) {
                final char ch = q.charAt(p);

                if (ch == '"') {
                    myQueryInQuotes = !myQueryInQuotes;
                    p += 1;
                } else if (ch == '(') {
                    myPosition = p + 1;
                    return myToken = TKN_LPAR;
                } else if (ch == ')') {
                    myPosition = p + 1;
                    return myToken = TKN_RPAR;
                } else if (isWordChar(ch)) {
                    myWordPosition = p;

                    while (++p < len && isWordChar(q.charAt(p))) {
                    }

                    String word = q.substring(myWordPosition, p);
                    myPosition = p;

                    if (word.equals(AND)) {
                        return myToken = TKN_AND;
                    } else if (word.equals(OR)) {
                        return myToken = TKN_OR;
                    } else if (word.equals(NOT)) {
                        return myToken = TKN_NOT;
                    } else {
                        word = word.toLowerCase();

                        if (!isStopWord(word)) {
                            if (!myQueryInQuotes) {
                                // just get the first normal form and ignore all other alternatives
                                word = getNormalForms(word, myLanguage)[0];
                            }

                            myWord = word;
                            return myToken = TKN_WORD;
                        }
                    }
                } else {
                    p += 1;
                }
            }

            myPosition = p;
            return myToken = TKN_EOQ;
        }
    }

}
