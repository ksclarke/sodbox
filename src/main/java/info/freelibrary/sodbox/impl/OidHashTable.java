
package info.freelibrary.sodbox.impl;

public interface OidHashTable {

    void clear();

    void clearDirty(Object obj);

    void flush();

    Object get(int oid);

    void invalidate();

    void put(int oid, Object obj);

    void reload();

    boolean remove(int oid);

    void setDirty(Object obj);

    int size();
}
