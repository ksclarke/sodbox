
package info.freelibrary.sodbox.fulltext;

/**
 * Occurrence of word in the document
 */
public class Occurrence implements Comparable {

    /**
     * Word (lower-cased)
     */
    public String myWord;

    /**
     * Position of word in document text (0 based)
     */
    public int myPosition;

    /**
     * Word occurrence kind. It is up to the document scanner implementation how to enumerate occurence kinds. These
     * is only one limitation - number of difference kinds should not exceed 8.
     */
    public int myKind;

    /**
     * Occurrence constructor
     *
     * @param aWord lower-cased word
     * @param aPosition offset of word from the beginning of document text
     * @param aKind word occurrence kind (should be less than 8)
     */
    public Occurrence(final String aWord, final int aPosition, final int aKind) {
        myWord = aWord;
        myPosition = aPosition;
        myKind = aKind;
    }

    @Override
    public int compareTo(final Object aObject) {
        final Occurrence occurrence = (Occurrence) aObject;

        int diff = myWord.compareTo(occurrence.myWord);

        if (diff == 0) {
            diff = myPosition - occurrence.myPosition;
        }

        return diff;
    }

}
