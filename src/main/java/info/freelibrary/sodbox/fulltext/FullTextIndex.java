
package info.freelibrary.sodbox.fulltext;

import java.io.Reader;
import java.util.Iterator;

import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.IResource;

/**
 * Full text search index. This index split document text in words, perform stemming of the words and build inverse
 * index. Full text index is able to execute search queries with logical operators (AND/OR/NOT) and strict match.
 * Returned results are ordered by rank, which includes inverse document frequency (IDF), frequency of word in the
 * document, occurrence kind and nearness of query keywords in the document text.
 */
public interface FullTextIndex extends IPersistent, IResource {

    /**
     * Add document to the index
     *
     * @param aObject document to be added
     */
    void add(FullTextSearchable aObject);

    /**
     * Add document to the index
     *
     * @param aObject document to be added
     * @param aText document text to be indexed
     * @param aLanguage language of the text
     */
    void add(Object aObject, Reader aText, String aLanguage);

    /**
     * Delete document from the index
     *
     * @param aObject document to be deleted
     */
    void delete(Object aObject);

    /**
     * Remove all elements from full text index
     */
    void clear();

    /**
     * Get iterator through full text index keywords started with specified prefix
     *
     * @param aPrefix keyword prefix (use empty string to get list of all keywords)
     * @return iterator through list of all keywords with specified prefix
     */
    Iterator<Keyword> getKeywords(String aPrefix);

    /**
     * Locate all documents containing words started with specified prefix
     *
     * @param aPrefix word prefix
     * @param aMaxResults maximal amount of selected documents
     * @param aTimeLimit limit for query execution time
     * @param aSort whether it is necessary to sort result by rank
     * @return result of query execution ordered by rank (if sort==true) or null in case of empty or incorrect query
     */
    FullTextSearchResult searchPrefix(String aPrefix, int aMaxResults, int aTimeLimit, boolean aSort);

    /**
     * Parse and execute full text search query
     *
     * @param aQuery text of the query
     * @param aLanguage language if the query
     * @param aMaxResults maximal amount of selected documents
     * @param aTimeLimit limit for query execution time
     * @return result of query execution ordered by rank or null in case of empty or incorrect query
     */
    FullTextSearchResult search(String aQuery, String aLanguage, int aMaxResults, int aTimeLimit);

    /**
     * Execute full text search query
     *
     * @param aQuery prepared query
     * @param aMaxResults maximal amount of selected documents
     * @param aTimeLimit limit for query execution time
     * @return result of query execution ordered by rank or null in case of empty or incorrect query
     */
    FullTextSearchResult search(FullTextQuery aQuery, int aMaxResults, int aTimeLimit);

    /**
     * Get total number of different words in all documents
     */
    int getNumberOfWords();

    /**
     * Get total number of indexed documents
     */
    int getNumberOfDocuments();

    /**
     * Get full text search helper
     */
    FullTextSearchHelper getHelper();

    /**
     * Description of full text index keyword
     */
    public interface Keyword {

        /**
         * Normal form of the keyword
         */
        String getNormalForm();

        /**
         * Number of keyword occurrences (number of documents containing this keyword)
         */
        long getNumberOfOccurrences();
    }

}
