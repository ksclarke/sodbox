package info.freelibrary.sodbox.fulltext;

import info.freelibrary.sodbox.IPersistent;

import java.io.Reader;

/**
 * Interface for classes which are able to extract text and its language themselves.
 */
public interface FullTextSearchable extends IPersistent
{
    /**
     * Get document text
     */
    Reader getText();

    /**
     * Get document language (null if unknown)
     */
    String getLanguage();
}