
package info.freelibrary.sodbox;

import java.util.Map;

/**
 * Interface of persistent hash table.
 */
public interface IPersistentHash<K, V> extends Map<K, V>, IPersistent, IResource {

    /**
     * Get entry for the specified key. This method can be used to obtains both key and value. It is needed when key
     * is persistent object.
     * 
     * @param aKey searched key
     * @return entry associated with this key or null if there is no such key in the map
     */
    Entry<K, V> getEntry(Object aKey);

}
