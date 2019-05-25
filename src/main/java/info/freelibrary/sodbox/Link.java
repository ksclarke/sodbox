
package info.freelibrary.sodbox;

import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

/**
 * Interface for one-to-many relation. There are two types of relations: embedded (when references to the related
 * objects are stored in relation owner object itself) and standalone (when relation is separate object, which
 * contains the reference to the relation owner and relation members). Both kinds of relations implements Link
 * interface. Embedded relation is created by Storage.createLink method and standalone relation is represented by
 * Relation persistent class created by Storage.createRelation method.
 */
public interface Link<T> extends List<T>, RandomAccess {

    /**
     * Set number of the linked objects.
     *
     * @param aNewSize new number of linked objects (if it is greater than original number, than extra elements will be
     *        set to null)
     */
    void setSize(int aNewSize);

    /**
     * Returns <tt>true</tt> if there are no related object.
     *
     * @return <tt>true</tt> if there are no related object
     */
    @Override
    boolean isEmpty();

    /**
     * Get related object by index.
     *
     * @param aIndex index of the object in the relation
     * @return referenced object
     */
    @Override
    T get(int aIndex);

    /**
     * Get related object by index without loading it. Returned object can be used only to get it OID or to compare
     * with other objects using <code>equals</code> method.
     *
     * @param aIndex index of the object in the relation
     * @return stub representing referenced object
     */
    Object getRaw(int aIndex);

    /**
     * Replace i-th element of the relation.
     *
     * @param aIndex index in the relation
     * @param aObject object to be included in the relation
     * @return the element previously at the specified position.
     */
    @Override
    T set(int aIndex, T aObject);

    /**
     * Assign value to i-th element of the relation. Unlike Link.set methods this method doesn't return previous value
     * of the element and so is faster if previous element value is not needed (it has not to be fetched from the
     * database).
     *
     * @param aIndex index in the relation
     * @param aObject object to be included in the relation
     */
    void setObject(int aIndex, T aObject);

    /**
     * Remove object with specified index from the relation Unlike Link.remove method this method doesn't return
     * removed element and so is faster if it is not needed (it has not to be fetched from the database).
     *
     * @param aIndex index in the relation
     */
    void removeObject(int aIndex);

    /**
     * Insert new object in the relation.
     *
     * @param aIndex insert position, should be in [0,size()]
     * @param aObject object inserted in the relation
     */
    void insert(int aIndex, T aObject);

    /**
     * Add all elements of the array to the relation.
     *
     * @param aArray array of objects which should be added to the relation
     */
    void addAll(T[] aArray);

    /**
     * Add specified elements of the array to the relation.
     *
     * @param aArray array of objects which should be added to the relation
     * @param aFrom index of the first element in the array to be added to the relation
     * @param aLength number of elements in the array to be added in the relation
     */
    void addAll(T[] aArray, int aFrom, int aLength);

    /**
     * Add all object members of the other relation to this relation.
     *
     * @param aLink another relation
     */
    boolean addAll(Link<T> aLink);

    /**
     * Return array with relation members. Members are not loaded and size of the array can be greater than actual
     * number of members.
     *
     * @return array of object with relation members used in implementation of Link class
     */
    Object[] toRawArray();

    /**
     * Get all relation members as array. The runtime type of the returned array is that of the specified array. If
     * the index fits in the specified array, it is returned therein. Otherwise, a new array is allocated with the
     * runtime type of the specified array and the size of this index. If this index fits in the specified array with
     * room to spare (i.e., the array has more elements than this index), the element in the array immediately
     * following the end of the index is set to <tt>null</tt>. This is useful in determining the length of this index
     * <i>only</i> if the caller knows that this index does not contain any <tt>null</tt> elements).
     *
     * @return array of object with relation members
     */
    @Override
    @SuppressWarnings("hiding")
    <T> T[] toArray(T[] aArray);

    /**
     * Checks if relation contains specified object instance.
     *
     * @param aObject specified object
     * @return <code>true</code> if object is present in the collection, <code>false</code> otherwise
     */
    boolean containsObject(T aObject);

    /**
     * Check if i-th element of Link is the same as specified object.
     *
     * @param aIndex element index
     * @param aObject object to compare with
     * @return <code>true</code> if i-th element of Link reference the same object as aObject
     */
    boolean containsElement(int aIndex, T aObject);

    /**
     * Get index of the specified object instance in the relation. This method use comparison by object identity
     * (instead of equals() method) and is significantly faster than List.indexOf() method.
     *
     * @param aObject specified object instance
     * @return zero based index of the object or -1 if object is not in the relation
     */
    int indexOfObject(Object aObject);

    /**
     * Get index of the specified object instance in the relation This method use comparison by object identity
     * (instead of equals() method) and is significantly faster than List.indexOf() method.
     *
     * @param aObject specified object instance
     * @return zero based index of the object or -1 if object is not in the relation
     */
    int lastIndexOfObject(Object aObject);

    /**
     * Remove all members from the relation.
     */
    @Override
    void clear();

    /**
     * Get iterator through link members This iterator supports remove() method.
     *
     * @return iterator through linked objects
     */
    @Override
    Iterator<T> iterator();

    /**
     * Replace all direct references to linked objects with stubs. This method is needed two avoid memory exhaustion
     * in case when there is a large number of objects in database, mutually referencing each other (each object can
     * directly or indirectly be accessed from other objects).
     */
    void unpin();

    /**
     * Replace references to elements with direct references. It will improve speed of manipulations with links, but
     * it can cause recursive loading in memory large number of objects and as a result - memory overflow, because
     * garbage collector will not be able to collect them.
     */
    void pin();

    /**
     * Remove all objects from the index and deallocate them. This method is equivalent to the following peace of
     * code: { Iterator i = index.iterator(); while (i.hasNext()) ((IPersistent)i.next()).deallocate(); index.clear();
     * } Please notice that this method doesn't check if there are some other references to the deallocated objects.
     * If deallocated object is included in some other index or is referenced from some other objects, then after
     * deallocation there will be dangling references and dereferencing them can cause unpredictable behavior of the
     * program.
     */
    void deallocateMembers();

}
