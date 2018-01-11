
package info.freelibrary.sodbox.fulltext;

import java.io.Reader;

import info.freelibrary.sodbox.IPersistent;

/**
 * Interface for classes which are able to extract text and its language themselves.
 */
public interface FullTextSearchable extends IPersistent {

    /**
     * Get document language (null if unknown)
     */
    String getLanguage();

    /**
     * Get document text
     */
    Reader getText();
}