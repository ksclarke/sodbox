
package info.freelibrary.sodbox;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This class provide small embedded map (collection of &lt;key,value&gt; pairs). Pairs are stored in the array in the
 * order of their insertion. Consequently operations with this map has linear complexity.
 */
public class SmallMap<K, V> extends PersistentResource implements Map<K, V> {

    transient volatile Set<K> myKeySet;

    transient volatile Collection<V> myValues;

    transient volatile Set<Map.Entry<K, V>> myEntrySet;

    private Pair<K, V>[] myPairs;

    /**
     * Creates a small map.
     */
    @SuppressWarnings("unchecked")
    public SmallMap() {
        myPairs = new Pair[0];
    }

    /**
     * Gets size of map.
     */
    @Override
    public int size() {
        return myPairs.length;
    }

    @Override
    public boolean isEmpty() {
        return myPairs.length == 0;
    }

    @Override
    public boolean containsKey(final Object aKey) {
        return getEntry(aKey) != null;
    }

    @Override
    public boolean containsValue(final Object aValue) {
        for (int i = 0; i < myPairs.length; i++) {
            if (myPairs[i].myValue == aValue || aValue != null && aValue.equals(myPairs[i].myValue)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public V get(final Object aKey) {
        final Entry<K, V> entry = getEntry(aKey);
        return entry != null ? entry.getValue() : null;
    }

    /**
     * Gets the entry for the supplied key.
     *
     * @param aKey A entry key
     * @return An entry
     */
    public Entry<K, V> getEntry(final Object aKey) {
        for (int i = 0; i < myPairs.length; i++) {
            if (myPairs[i].myKey.equals(aKey)) {
                return myPairs[i];
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(final K aKey, final V aValue) {
        final int n = myPairs.length;

        if (aKey == null) {
            throw new IllegalArgumentException("Key is null");
        }

        for (int i = 0; i < n; i++) {
            if (aKey.equals(myPairs[i].myKey)) {
                final V oldValue = myPairs[i].myValue;

                myPairs[i].myValue = aValue;
                modify();

                return oldValue;
            }
        }

        final Pair<K, V>[] newPairs = new Pair[n + 1];

        System.arraycopy(myPairs, 0, newPairs, 0, n);
        newPairs[n] = new Pair<>(aKey, aValue);
        myPairs = newPairs;
        modify();

        return null;
    }

    @Override
    public V remove(final Object aKey) {
        final Entry<K, V> e = removeEntry(aKey);
        return e == null ? null : e.getValue();
    }

    /**
     * Remove an entry at the supplied index.
     *
     * @param aIndex The index of the entry to remove
     * @return The entry that's been removed
     */
    @SuppressWarnings("unchecked")
    public Entry<K, V> removeAt(final int aIndex) {
        final Pair<K, V> pair = myPairs[aIndex];
        final Pair<K, V>[] newPairs = new Pair[myPairs.length - 1];

        System.arraycopy(myPairs, 0, newPairs, 0, aIndex);
        System.arraycopy(myPairs, aIndex + 1, newPairs, aIndex, myPairs.length - aIndex - 1);
        myPairs = newPairs;
        modify();

        return pair;
    }

    final Entry<K, V> removeEntry(final Object aKey) {
        for (int i = 0; i < myPairs.length; i++) {
            if (aKey.equals(myPairs[i].myKey)) {
                return removeAt(i);
            }
        }

        return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> aMap) {
        for (final Iterator<? extends Map.Entry<? extends K, ? extends V>> i = aMap.entrySet().iterator(); i
                .hasNext();) {
            final Map.Entry<? extends K, ? extends V> e = i.next();
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void clear() {
        myPairs = new Pair[0];
        modify();
    }

    @Override
    public Set<K> keySet() {
        final Set<K> ks = myKeySet;

        return ks != null ? ks : (myKeySet = new KeySet());
    }

    @Override
    public Collection<V> values() {
        final Collection<V> vs = myValues;

        return vs != null ? vs : (myValues = new Values());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        final Set<Map.Entry<K, V>> es = myEntrySet;

        return es != null ? es : (myEntrySet = new EntrySet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(final Object aObject) {
        if (aObject == this) {
            return true;
        }

        if (!(aObject instanceof Map)) {
            return false;
        }

        final Map<K, V> m = (Map<K, V>) aObject;

        if (m.size() != size()) {
            return false;
        }

        final Iterator<Entry<K, V>> i = entrySet().iterator();

        while (i.hasNext()) {
            final Entry<K, V> e = i.next();
            final K key = e.getKey();
            final V value = e.getValue();

            if (value == null) {
                if (!(m.get(key) == null && m.containsKey(key))) {
                    return false;
                }
            } else {
                if (!value.equals(m.get(key))) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        final Iterator<Entry<K, V>> i = entrySet().iterator();

        while (i.hasNext()) {
            h += i.next().hashCode();
        }

        return h;
    }

    @Override
    public String toString() {
        final Iterator<Entry<K, V>> i = entrySet().iterator();

        if (!i.hasNext()) {
            return "{}";
        }

        final StringBuilder sb = new StringBuilder();

        sb.append('{');

        while (true) {
            final Entry<K, V> e = i.next();
            final K key = e.getKey();
            final V value = e.getValue();
            final String thisMap = "(this Map)";

            sb.append(key == this ? thisMap : key);
            sb.append('=');
            sb.append(value == this ? thisMap : value);

            if (!i.hasNext()) {
                return sb.append('}').toString();
            }

            sb.append(", ");
        }
    }

    private final class KeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        @Override
        public int size() {
            return myPairs.length;
        }

        @Override
        public boolean contains(final Object aObject) {
            return containsKey(aObject);
        }

        @Override
        public boolean remove(final Object aObject) {
            return SmallMap.this.removeEntry(aObject) != null;
        }

        @Override
        public void clear() {
            SmallMap.this.clear();
        }
    }

    private final class EntryIterator extends ArrayIterator<Map.Entry<K, V>> {

        @Override
        public Map.Entry<K, V> next() {
            return nextEntry();
        }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(final Object aObject) {
            if (!(aObject instanceof Map.Entry<?, ?>)) {
                return false;
            }

            final Map.Entry<K, V> e = (Map.Entry<K, V>) aObject;
            final Entry<K, V> candidate = getEntry(e.getKey());

            return candidate != null && candidate.equals(e);
        }

        @Override
        public boolean remove(final Object aObject) {
            final Entry<K, V> pair = getEntry(aObject);

            if (pair != null && pair.equals(aObject)) {
                removeEntry(aObject);

                return true;
            }

            return false;
        }

        @Override
        public int size() {
            return myPairs.length;
        }

        @Override
        public void clear() {
            SmallMap.this.clear();
        }
    }

    static class Pair<K, V> implements Map.Entry<K, V>, IValue {

        K myKey;

        V myValue;

        Pair() {
        }

        Pair(final K aKey, final V aValue) {
            myValue = aValue;
            myKey = aKey;
        }

        @Override
        public final K getKey() {
            return myKey;
        }

        @Override
        public final V getValue() {
            return myValue;
        }

        /**
         * In case of updating pair value using this method it is necessary to explicitly call modify() method for the
         * parent SmallMap object
         */
        @Override
        public final V setValue(final V aNewValue) {
            final V oldValue = myValue;

            myValue = aNewValue;

            return oldValue;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final boolean equals(final Object aObject) {
            if (!(aObject instanceof Map.Entry<?, ?>)) {
                return false;
            }

            final Map.Entry<K, V> e = (Map.Entry<K, V>) aObject;
            final Object k1 = getKey();
            final Object k2 = e.getKey();

            if (k1 == k2 || k1 != null && k1.equals(k2)) {
                final Object v1 = getValue();
                final Object v2 = e.getValue();

                if (v1 == v2 || v1 != null && v1.equals(v2)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public final int hashCode() {
            return myKey.hashCode() ^ (myValue == null ? 0 : myValue.hashCode());
        }

        @Override
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }

    private abstract class ArrayIterator<E> implements Iterator<E> {

        int myCurrent;

        ArrayIterator() {
            myCurrent = -1;
        }

        @Override
        public final boolean hasNext() {
            return myCurrent + 1 < myPairs.length;
        }

        final Entry<K, V> nextEntry() {
            if (myCurrent + 1 >= myPairs.length) {
                throw new NoSuchElementException();
            }

            return myPairs[++myCurrent];
        }

        @Override
        public void remove() {
            if (myCurrent < 0 || myCurrent >= myPairs.length) {
                throw new IllegalStateException();
            }

            SmallMap.this.removeAt(myCurrent--);
        }
    }

    private final class ValueIterator extends ArrayIterator<V> {

        @Override
        public V next() {
            return nextEntry().getValue();
        }
    }

    private final class KeyIterator extends ArrayIterator<K> {

        @Override
        public K next() {
            return nextEntry().getKey();
        }
    }

    private final class Values extends AbstractCollection<V> {

        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        @Override
        public int size() {
            return myPairs.length;
        }

        @Override
        public boolean contains(final Object aObject) {
            return containsValue(aObject);
        }

        @Override
        public void clear() {
            SmallMap.this.clear();
        }
    }

}
