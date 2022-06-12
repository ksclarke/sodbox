
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Relation;

public class RelationImpl<M, O> extends Relation<M, O> {

    Link<M> myLink;

    RelationImpl() {
    }

    RelationImpl(final StorageImpl aStorage, final O aOwner) {
        super(aOwner);
        myLink = new LinkImpl<>(aStorage, 8);
    }

    @Override
    public int size() {
        return myLink.size();
    }

    @Override
    public void setSize(final int aNewSize) {
        myLink.setSize(aNewSize);
    }

    @Override
    public boolean isEmpty() {
        return myLink.isEmpty();
    }

    @Override
    public boolean remove(final Object aObject) {
        return myLink.remove(aObject);
    }

    @Override
    public M get(final int aIndex) {
        return myLink.get(aIndex);
    }

    @Override
    public Object getRaw(final int aIndex) {
        return myLink.getRaw(aIndex);
    }

    @Override
    public M set(final int aIndex, final M aObject) {
        return myLink.set(aIndex, aObject);
    }

    @Override
    public void setObject(final int aIndex, final M aObject) {
        myLink.setObject(aIndex, aObject);
    }

    @Override
    public void removeObject(final int aIndex) {
        myLink.removeObject(aIndex);
    }

    @Override
    public M remove(final int aIndex) {
        return myLink.remove(aIndex);
    }

    @Override
    public void insert(final int aIndex, final M aObject) {
        myLink.insert(aIndex, aObject);
    }

    @Override
    public void add(final int aIndex, final M aObject) {
        myLink.add(aIndex, aObject);
    }

    @Override
    public boolean add(final M aObject) {
        return myLink.add(aObject);
    }

    @Override
    public void addAll(final M[] aArray) {
        myLink.addAll(aArray);
    }

    @Override
    public void addAll(final M[] aArray, final int aFrom, final int aLength) {
        myLink.addAll(aArray, aFrom, aLength);
    }

    @Override
    public boolean addAll(final Link<M> aLink) {
        return myLink.addAll(aLink);
    }

    @Override
    public Object[] toArray() {
        return myLink.toArray();
    }

    @Override
    public Object[] toRawArray() {
        return myLink.toRawArray();
    }

    @Override
    public <T> T[] toArray(final T[] aArray) {
        return myLink.<T>toArray(aArray);
    }

    @Override
    public boolean contains(final Object aObject) {
        return myLink.contains(aObject);
    }

    @Override
    public boolean containsObject(final M aObject) {
        return myLink.containsObject(aObject);
    }

    @Override
    public int indexOf(final Object aObject) {
        return myLink.indexOf(aObject);
    }

    @Override
    public int lastIndexOf(final Object aObject) {
        return myLink.lastIndexOf(aObject);
    }

    @Override
    public int indexOfObject(final Object aObject) {
        return myLink.indexOfObject(aObject);
    }

    @Override
    public int lastIndexOfObject(final Object aObject) {
        return myLink.lastIndexOfObject(aObject);
    }

    @Override
    public void clear() {
        myLink.clear();
    }

    @Override
    public void deallocateMembers() {
        myLink.deallocateMembers();
    }

    @Override
    public Iterator<M> iterator() {
        return myLink.iterator();
    }

    @Override
    public boolean containsAll(final Collection<?> aCollection) {
        return myLink.containsAll(aCollection);
    }

    @Override
    public boolean containsElement(final int aIndex, final M aObject) {
        return myLink.containsElement(aIndex, aObject);
    }

    @Override
    public boolean addAll(final Collection<? extends M> aCollection) {
        return myLink.addAll(aCollection);
    }

    @Override
    public boolean addAll(final int aIndex, final Collection<? extends M> aCollection) {
        return myLink.addAll(aIndex, aCollection);
    }

    @Override
    public boolean removeAll(final Collection<?> aCollection) {
        return myLink.removeAll(aCollection);
    }

    @Override
    public boolean retainAll(final Collection<?> aCollection) {
        return myLink.retainAll(aCollection);
    }

    @Override
    public void pin() {
        myLink.pin();
    }

    @Override
    public void unpin() {
        myLink.unpin();
    }

    @Override
    public List<M> subList(final int aFromIndex, final int aToIndex) {
        return myLink.subList(aFromIndex, aToIndex);
    }

    @Override
    public ListIterator<M> listIterator(final int aIndex) {
        return myLink.listIterator(aIndex);
    }

    @Override
    public ListIterator<M> listIterator() {
        return myLink.listIterator();
    }

}
