
package info.freelibrary.sodbox;

import java.util.ArrayList;

/**
 * PATRICIA trie (Practical Algorithm To Retrieve Information Coded In Alphanumeric). Tries are a kind of tree where
 * each node holds a common part of one or more keys. PATRICIA trie is one of the many existing variants of the trie,
 * which adds path compression by grouping common sequences of nodes together.<br/>
 * This structure provides a very efficient way of storing values while maintaining the lookup time for a key in O(N)
 * in the worst case, where N is the length of the longest key. This structure has it's main use in IP routing
 * software, but can provide an interesting alternative to other structures such as hashtables when memory space is of
 * concern.
 */
public interface PatriciaTrie<T> extends IPersistent, IResource {

    /**
     * Add new key to the trie.
     * 
     * @param aKey bit vector
     * @param aObject persistent object associated with this key
     * @return previous object associated with this key or <code>null</code> if there was no such object
     */
    T add(PatriciaTrieKey aKey, T aObject);

    /**
     * Find best match with specified key.
     * 
     * @param aKey bit vector
     * @return object associated with this deepest possible match with specified key
     */
    T findBestMatch(PatriciaTrieKey aKey);

    /**
     * Find exact match with specified key.
     * 
     * @param aKey bit vector
     * @return object associated with this key or NULL if match is not found
     */
    T findExactMatch(PatriciaTrieKey aKey);

    /**
     * Removes key from the triesKFind exact match with specified key.
     * 
     * @param aKey bit vector
     * @return object associated with removed key or <code>null</code> if such key is not found
     */
    T remove(PatriciaTrieKey aKey);

    /**
     * Get list of all elements in the Trie.
     * 
     * @return list of all elements
     */
    ArrayList<T> elements();

}
