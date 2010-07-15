package info.freelibrary.sodbox;

import java.util.Iterator;

public class IteratorWrapper<T> extends IterableIterator<T> {

	private Iterator<T> myIterator;

	public IteratorWrapper(Iterator<T> aIterator) {
		myIterator = aIterator;
	}

	public boolean hasNext() {
		return myIterator.hasNext();
	}

	public T next() {
		return myIterator.next();
	}

	public void remove() {
		myIterator.remove();
	}
}
