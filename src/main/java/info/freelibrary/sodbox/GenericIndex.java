
package info.freelibrary.sodbox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface of object index. This is base interface for Index and FieldIndex, allowing to write generic algorithms
 * working with both types of indices.
 */
public interface GenericIndex<T> extends IPersistent, IResource, Collection<T> {

    /**
     * Ascent order.
     */
    int ASCENT_ORDER = 0;

    /**
     * Descent order.
     */
    int DESCENT_ORDER = 1;

    /**
     * Get object by key (exact match)
     *
     * @param aKey specified key. It should match with type of the index and should be inclusive.
     * @return object with this value of the key or <code>null</code> if key not found
     * @throws StorageError KEY_NOT_UNIQUE exception if there are more than one objects in the index with specified
     *         value of the key.
     */
    T get(Key aKey);

    /**
     * Get objects which key value belongs to the specified range. Either from boundary, either till boundary either
     * both of them can be <code>null</code>. In last case the method returns all objects from the index.
     *
     * @param aFrom low boundary. If <code>null</code> then low boundary is not specified. Low boundary can be
     *        inclusive or exclusive.
     * @param aTo high boundary. If <code>null</code> then high boundary is not specified. High boundary can be
     *        inclusive or exclusive.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    ArrayList<T> getList(Key aFrom, Key aTo);

    /**
     * Get objects which key value belongs to the specified range. Either from boundary, either till boundary either
     * both of them can be <code>null</code>. In last case the method returns all objects from the index.
     *
     * @param aFrom low boundary. If <code>null</code> then low boundary is not specified. Low boundary can be
     *        inclusive or exclusive.
     * @param aTo high boundary. If <code>null</code> then high boundary is not specified. High boundary can be
     *        inclusive or exclusive.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    Object[] get(Key aFrom, Key aTo);

    /**
     * Get object by string key (exact match).
     *
     * @param aKey packed key
     * @return object with this value of the key or <code>null</code> if key not found
     * @throws StorageError KEY_NOT_UNIQUE exception if there are more than one objects in the index with specified
     *         value of the key.
     */
    T get(Object aKey);

    /**
     * Get objects which key value belongs to the specified range. Either from boundary, either till boundary either
     * both of them can be <code>null</code>. In last case the method returns all objects from the index.
     *
     * @param aFrom inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param aTo inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    Object[] get(Object aFrom, Object aTo);

    /**
     * Get objects which key value belongs to the specified range. Either from boundary, either till boundary either
     * both of them can be <code>null</code>. In last case the method returns all objects from the index.
     *
     * @param aFrom inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param aTo inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    ArrayList<T> getList(Object aFrom, Object aTo);

    /**
     * Get objects with objects with key started with specified prefix, i.e. getPrefix("abc") will return "abc",
     * "abcd", "abcdef", ... but not "ab".
     *
     * @param aPrefix string key prefix
     * @return array of objects which key starts with this prefix
     */
    Object[] getPrefix(String aPrefix);

    /**
     * Get objects with string key prefix.
     *
     * @param aPrefix string key prefix
     * @return list of objects which key starts with this prefix
     */
    ArrayList<T> getPrefixList(String aPrefix);

    /**
     * Locate all objects which key is prefix of specified word, i.e. prefixSearch("12345") will return "12", "123",
     * "1234", "12345", but not "123456".
     *
     * @param aWord string which prefixes are located in index
     * @return array of objects which key is prefix of specified word, ordered by key value
     */
    Object[] prefixSearch(String aWord);

    /**
     * Locate all objects which key is prefix of specified word.
     *
     * @param aWord string which prefixes are located in index
     * @return list of objects which key is prefix of specified word, ordered by key value
     */
    ArrayList<T> prefixSearchList(String aWord);

    /**
     * Get iterator for traversing all objects in the index. Objects are iterated in the ascent key order. This
     * iterator supports remove() method. To make it possible to update, remove or add members to the index during
     * iteration it is necessary to set "sodbox.concurrent.iterator" property (by default it is not supported because
     * it cause extra overhead during iteration).
     *
     * @return index iterator
     */
    @Override
    Iterator<T> iterator();

    /**
     * Get iterator for traversing all entries in the index. Iterator next() method returns object implementing
     * <code>Map.Entry</code> interface which allows to get entry key and value. Objects are iterated in the ascent
     * key order. This iterator supports remove() method. To make it possible to update, remove or add members to the
     * index during iteration it is necessary to set "sodbox.concurrent.iterator" property (by default it is not
     * supported because it cause extra overhead during iteration).
     *
     * @return index entries iterator
     */
    IterableIterator<Map.Entry<Object, T>> entryIterator();

    /**
     * Get iterator for traversing objects in the index with key belonging to the specified range. This iterator
     * supports remove() method. To make it possible to update, remove or add members to the index during iteration it
     * is necessary to set "sodbox.concurrent.iterator" property (by default it is not supported because it cause
     * extra overhead during iteration).
     *
     * @param aFrom low boundary. If <code>null</code> then low boundary is not specified. Low boundary can be
     *        inclusive or exclusive.
     * @param aTo high boundary. If <code>null</code> then high boundary is not specified. High boundary can be
     *        inclusive or exclusive.
     * @param aOrder <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    IterableIterator<T> iterator(Key aFrom, Key aTo, int aOrder);

    /**
     * Get iterator for traversing objects in the index with key belonging to the specified range. This iterator
     * supports remove() method. To make it possible to update, remove or add members to the index during iteration it
     * is necessary to set "sodbox.concurrent.iterator" property (by default it is not supported because it cause
     * extra overhead during iteration).
     *
     * @param aFrom inclusive low boundary. If <code>null</code> then low boundary is not specified. Low boundary can
     *        be inclusive or exclusive.
     * @param aTo inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param aOrder <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    IterableIterator<T> iterator(Object aFrom, Object aTo, int aOrder);

    /**
     * Get iterator for traversing index entries with key belonging to the specified range. Iterator next() method
     * returns object implementing <code>Map.Entry</code> interface This iterator supports remove() method. To make it
     * possible to update, remove or add members to the index during iteration it is necessary to set
     * "sodbox.concurrent.iterator" property (by default it is not supported because it cause extra overhead during
     * iteration).
     *
     * @param aFrom low boundary. If <code>null</code> then low boundary is not specified. Low boundary can be
     *        inclusive or exclusive.
     * @param aTo high boundary. If <code>null</code> then high boundary is not specified. High boundary can be
     *        inclusive or exclusive.
     * @param aOrder <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    IterableIterator<Map.Entry<Object, T>> entryIterator(Key aFrom, Key aTo, int aOrder);

    /**
     * Get iterator for traversing index entries with key belonging to the specified range. Iterator next() method
     * returns object implementing <code>Map.Entry</code> interface This iterator supports remove() method. To make it
     * possible to update, remove or add members to the index during iteration it is necessary to set
     * "sodbox.concurrent.iterator" property (by default it is not supported because it cause extra overhead during
     * iteration).
     *
     * @param aFrom inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param aTo inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param aOrder <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    IterableIterator<Map.Entry<Object, T>> entryIterator(Object aFrom, Object aTo, int aOrder);

    /**
     * Get iterator for records which keys started with specified prefix Objects are iterated in the ascent key order.
     * This iterator supports remove() method. To make it possible to update, remove or add members to the index
     * during iteration it is necessary to set "sodbox.concurrent.iterator" property (by default it is not supported
     * because it cause extra overhead during iteration).
     *
     * @param aPrefix key prefix
     * @return selection iterator
     */
    IterableIterator<T> prefixIterator(String aPrefix);

    /**
     * Get ascent or descent iterator for records which keys started with specified prefix Objects are iterated in the
     * ascent key order. This iterator supports remove() method. To make it possible to update, remove or add members
     * to the index during iteration it is necessary to set "sodbox.concurrent.iterator" property (by default it is
     * not supported because it cause extra overhead during iteration).
     *
     * @param aPrefix key prefix
     * @param aOrder <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    IterableIterator<T> prefixIterator(String aPrefix, int aOrder);

    /**
     * Get type of index key.
     *
     * @return type of index key
     */
    Class getKeyType();

    /**
     * Get types of index compound key components.
     *
     * @return array of types of compound key components
     */
    Class[] getKeyTypes();

    /**
     * Get element at specified position. This method is efficient only for random access indices.
     *
     * @param aIndex position of element in the index
     * @return object at specified position
     * @exception IndexOutOfBoundsException if position is less than 0 or greater or equal than index size
     */
    T getAt(int aIndex);

    /**
     * Get position of the first element with specified key. This method is efficient only for random access indices.
     *
     * @param aKey located key
     * @return position of the first element with this key or -1 if no such element is found
     */
    int indexOf(Key aKey);

    /**
     * Get entry iterator of objects in the index starting with specified position. This method is efficient only for
     * random access indices. You should not update/remove or add members to the index during iteration.
     *
     * @param aStart Start position in the index. First <code>pos</code> elements will be skipped.
     * @param aOrder <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return index entries iterator
     */
    IterableIterator<Map.Entry<Object, T>> entryIterator(int aStart, int aOrder);

    /**
     * Check if index is unique.
     *
     * @return true if index doesn't allow duplicates
     */
    boolean isUnique();

    /**
     * Remove all objects from the index and deallocate them. This method is equivalent to the following peace of
     * code: <code>
     * {
     *   Iterator i = index.iterator();
     *   while (i.hasNext()) {
     *     ((IPersistent)i.next()).deallocate(); index.clear();
     *   }
     * }
     * </code> Please notice that this method doesn't check if there are some other references to the deallocated
     * objects. If deallocated object is included in some other index or is referenced from some other objects, then
     * after deallocation there will be dangling references and dereferencing them can cause unpredictable behavior of
     * the program.
     */
    void deallocateMembers();

}
