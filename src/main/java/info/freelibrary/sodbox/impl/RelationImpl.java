
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Relation;

public class RelationImpl<M, O> extends Relation<M, O> {

    Link<M> link;

    RelationImpl() {
    }

    RelationImpl(final StorageImpl db, final O owner) {
        super(owner);
        link = new LinkImpl<M>(db, 8);
    }

    @Override
    public void add(final int i, final M obj) {
        link.add(i, obj);
    }

    @Override
    public boolean add(final M obj) {
        return link.add(obj);
    }

    @Override
    public boolean addAll(final Collection<? extends M> c) {
        return link.addAll(c);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends M> c) {
        return link.addAll(index, c);
    }

    @Override
    public boolean addAll(final Link<M> anotherLink) {
        return link.addAll(anotherLink);
    }

    @Override
    public void addAll(final M[] arr) {
        link.addAll(arr);
    }

    @Override
    public void addAll(final M[] arr, final int from, final int length) {
        link.addAll(arr, from, length);
    }

    @Override
    public void clear() {
        link.clear();
    }

    @Override
    public boolean contains(final Object obj) {
        return link.contains(obj);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return link.containsAll(c);
    }

    @Override
    public boolean containsElement(final int i, final M obj) {
        return link.containsElement(i, obj);
    }

    @Override
    public boolean containsObject(final M obj) {
        return link.containsObject(obj);
    }

    @Override
    public void deallocateMembers() {
        link.deallocateMembers();
    }

    @Override
    public M get(final int i) {
        return link.get(i);
    }

    @Override
    public Object getRaw(final int i) {
        return link.getRaw(i);
    }

    @Override
    public int indexOf(final Object obj) {
        return link.indexOf(obj);
    }

    @Override
    public int indexOfObject(final Object obj) {
        return link.indexOfObject(obj);
    }

    @Override
    public void insert(final int i, final M obj) {
        link.insert(i, obj);
    }

    @Override
    public boolean isEmpty() {
        return link.isEmpty();
    }

    @Override
    public Iterator<M> iterator() {
        return link.iterator();
    }

    @Override
    public int lastIndexOf(final Object obj) {
        return link.lastIndexOf(obj);
    }

    @Override
    public int lastIndexOfObject(final Object obj) {
        return link.lastIndexOfObject(obj);
    }

    @Override
    public ListIterator<M> listIterator() {
        return link.listIterator();
    }

    @Override
    public ListIterator<M> listIterator(final int index) {
        return link.listIterator(index);
    }

    @Override
    public void pin() {
        link.pin();
    }

    @Override
    public M remove(final int i) {
        return link.remove(i);
    }

    @Override
    public boolean remove(final Object o) {
        return link.remove(o);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return link.removeAll(c);
    }

    @Override
    public void removeObject(final int i) {
        link.removeObject(i);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return link.retainAll(c);
    }

    @Override
    public M set(final int i, final M obj) {
        return link.set(i, obj);
    }

    @Override
    public void setObject(final int i, final M obj) {
        link.setObject(i, obj);
    }

    @Override
    public void setSize(final int newSize) {
        link.setSize(newSize);
    }

    @Override
    public int size() {
        return link.size();
    }

    @Override
    public List<M> subList(final int fromIndex, final int toIndex) {
        return link.subList(fromIndex, toIndex);
    }

    @Override
    public Object[] toArray() {
        return link.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] arr) {
        return link.<T>toArray(arr);
    }

    @Override
    public Object[] toRawArray() {
        return link.toRawArray();
    }

    @Override
    public void unpin() {
        link.unpin();
    }
}
