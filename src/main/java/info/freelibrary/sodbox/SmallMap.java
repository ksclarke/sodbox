
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

    private abstract class ArrayIterator<E> implements Iterator<E> {

        int current;

        ArrayIterator() {
            current = -1;
        }

        @Override
        public final boolean hasNext() {
            return current + 1 < pairs.length;
        }

        final Entry<K, V> nextEntry() {
            if (current + 1 >= pairs.length) {
                throw new NoSuchElementException();
            }

            return pairs[++current];
        }

        @Override
        public void remove() {
            if (current < 0 || current >= pairs.length) {
                throw new IllegalStateException();
            }

            SmallMap.this.removeAt(current--);
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
        public void clear() {
            SmallMap.this.clear();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(final Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }

            final Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            final Entry<K, V> candidate = getEntry(e.getKey());

            return candidate != null && candidate.equals(e);
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public boolean remove(final Object o) {
            final Entry<K, V> pair = getEntry(o);

            if (pair != null && pair.equals(o)) {
                removeEntry(o);

                return true;
            }

            return false;
        }

        @Override
        public int size() {
            return pairs.length;
        }
    }

    private final class KeyIterator extends ArrayIterator<K> {

        @Override
        public K next() {
            return nextEntry().getKey();
        }
    }

    private final class KeySet extends AbstractSet<K> {

        @Override
        public void clear() {
            SmallMap.this.clear();
        }

        @Override
        public boolean contains(final Object o) {
            return containsKey(o);
        }

        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        @Override
        public boolean remove(final Object o) {
            return SmallMap.this.removeEntry(o) != null;
        }

        @Override
        public int size() {
            return pairs.length;
        }
    }

    static class Pair<K, V> implements Map.Entry<K, V>, IValue {

        K key;

        V value;

        Pair() {
        }

        Pair(final K k, final V v) {
            value = v;
            key = k;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final boolean equals(final Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }

            final Map.Entry<K, V> e = (Map.Entry<K, V>) o;
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
        public final K getKey() {
            return key;
        }

        @Override
        public final V getValue() {
            return value;
        }

        @Override
        public final int hashCode() {
            return key.hashCode() ^ (value == null ? 0 : value.hashCode());
        }

        /**
         * In case of updating pair value using this method it is necessary to explicitly call modify() method for the
         * parent SmallMap object
         */
        @Override
        public final V setValue(final V newValue) {
            final V oldValue = value;

            value = newValue;

            return oldValue;
        }

        @Override
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }

    private final class ValueIterator extends ArrayIterator<V> {

        @Override
        public V next() {
            return nextEntry().getValue();
        }
    }

    private final class Values extends AbstractCollection<V> {

        @Override
        public void clear() {
            SmallMap.this.clear();
        }

        @Override
        public boolean contains(final Object o) {
            return containsValue(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        @Override
        public int size() {
            return pairs.length;
        }
    }

    transient volatile Set<K> keySet;

    transient volatile Collection<V> values;

    transient volatile Set<Map.Entry<K, V>> entrySet;

    private Pair<K, V>[] pairs;

    @SuppressWarnings("unchecked")
    public SmallMap() {
        pairs = new Pair[0];
    }

    @Override
    @SuppressWarnings("unchecked")
    public void clear() {
        pairs = new Pair[0];
        modify();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getEntry(key) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        for (final Pair<K, V> pair : pairs) {
            if (pair.value == value || value != null && value.equals(pair.value)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        final Set<Map.Entry<K, V>> es = entrySet;

        return es != null ? es : (entrySet = new EntrySet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Map)) {
            return false;
        }

        final Map<K, V> m = (Map<K, V>) o;

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
    public V get(final Object key) {
        final Entry<K, V> entry = getEntry(key);

        return entry != null ? entry.getValue() : null;
    }

    public Entry<K, V> getEntry(final Object key) {
        for (final Pair<K, V> pair : pairs) {
            if (pair.key.equals(key)) {
                return pair;
            }
        }

        return null;
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
    public boolean isEmpty() {
        return pairs.length == 0;
    }

    @Override
    public Set<K> keySet() {
        final Set<K> ks = keySet;

        return ks != null ? ks : (keySet = new KeySet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(final K key, final V value) {
        final int n = pairs.length;

        if (key == null) {
            throw new IllegalArgumentException("Key is null");
        }

        for (int i = 0; i < n; i++) {
            if (key.equals(pairs[i].key)) {
                final V oldValue = pairs[i].value;

                pairs[i].value = value;
                modify();

                return oldValue;
            }
        }

        final Pair<K, V>[] newPairs = new Pair[n + 1];

        System.arraycopy(pairs, 0, newPairs, 0, n);
        newPairs[n] = new Pair<K, V>(key, value);
        pairs = newPairs;
        modify();

        return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public V remove(final Object key) {
        final Entry<K, V> e = removeEntry(key);

        return e == null ? null : e.getValue();
    }

    @SuppressWarnings("unchecked")
    public Entry<K, V> removeAt(final int i) {
        final Pair<K, V> pair = pairs[i];
        final Pair<K, V>[] newPairs = new Pair[pairs.length - 1];

        System.arraycopy(pairs, 0, newPairs, 0, i);
        System.arraycopy(pairs, i + 1, newPairs, i, pairs.length - i - 1);
        pairs = newPairs;
        modify();

        return pair;
    }

    final Entry<K, V> removeEntry(final Object key) {
        for (int i = 0; i < pairs.length; i++) {
            if (key.equals(pairs[i].key)) {
                return removeAt(i);
            }
        }

        return null;
    }

    @Override
    public int size() {
        return pairs.length;
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

            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);

            if (!i.hasNext()) {
                return sb.append('}').toString();
            }

            sb.append(", ");
        }
    }

    @Override
    public Collection<V> values() {
        final Collection<V> vs = values;

        return vs != null ? vs : (values = new Values());
    }

}
