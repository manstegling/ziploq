/*
 * Copyright (c) 2022 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An unordered collection optimized for fast insertion ({@link #add}) and
 * iteration including element removal ({@link Itr}). Constant time complexity is
 * achieved by traversing the internal array backwards and rearranging elements
 * only when needed. As the name suggest the iteration order is undefined.
 *
 * @author M Tegling
 *
 * @param <E> element type
 */
public class UnorderedCollection<E> extends AbstractCollection<E> {

	private static final int DEFAULT_SIZE = 8;
	private Object[] array; // lazy creation to prevent premature allocation
	private int size;

	/**
	 * Adds element to the list in O(1) complexity
	 * @param e element to be added
	 * @return always {@code true}, be mindful of possible overflow
	 */
	@Override
	public boolean add(E e) {
		if (array == null) {
			array = new Object[DEFAULT_SIZE];
		} else if (size >= array.length) {
			grow();
		}
		array[size++] = e;
		return true;
	}

	/**
	 * Removes first occurrence of the supplied element from the list in O(N) complexity
	 * @param e element to be removed
	 * @return {@code true} if an element was removed, {@code false} otherwise
	 */
	@Override
	public boolean remove(Object e) {
		for (int i = 0; i < size; i++) {
			if (array[i] == e) {
				array[i] = null;
				size--;
				return true;
			}
		}
		return false;
	}

	/**
	 * Determines whether the list contains the supplied element from the list in O(N) complexity
	 * @param e element to be checked
	 * @return {@code true} if the list contains the element, {@code false} otherwise
	 */
	@Override
	public boolean contains(Object e) {
		for (int i = 0; i < size; i++) {
			if (array[i] == e) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the number of elements in the list
	 * @return number of elements in the list
	 */
	@Override
	public int size() {
		return size;
	}

	/**
	 * Returns an efficient Iterator for the list. The iteration order is undefined but
	 * each element will of course be iterated over exactly once. The Iterator implements
	 * {@link Iterator#remove} and removing the current element is done in O(1) complexity.
	 * @return Iterator for this list with {@link Iterator#remove} capability
	 */
	@Override
	public Iterator<E> iterator() {
		return new Itr<>(this);
	}
	
	private void grow() {
		// grow by 50% each time (does not handle overflows)
		array = Arrays.copyOf(array, array.length + (array.length >> 1));
	}

	/**
	 * An optimized iterator. In particular, the {@link #remove} operation removes the current element
	 * and fills the gap with the tail element. Rearranging elements in this way is allowed since
	 * no iteration order is defined for {@code UnorderedCollection}. By iterating backwards over the
	 * backing array we can avoid checking whether elements have already been encountered.
	 *
	 * @param <E> element type
	 */
	private static class Itr<E> implements Iterator<E> {
		
		private final UnorderedCollection<E> list;
		private int cursor;
		private boolean canRemove;
		
		Itr(UnorderedCollection<E> list) {
			this.list = list;
			this.cursor = list.size - 1;
		}
		
		@Override
		public boolean hasNext() {
			return cursor != -1;
		}

		@Override
		public E next() {
			if (cursor < 0) {
				throw new NoSuchElementException("calling next() with no more elements");
			}
			canRemove = true;
			@SuppressWarnings("unchecked")
			E e = (E) list.array[cursor--];
			return e;
		}

		@Override
		public void remove() {
			if (!canRemove) {
				throw new IllegalStateException("Iterator misused, cannot remove element");
			}
			int endIdx = list.size - 1;
			int currIdx = cursor + 1;
			if (currIdx < endIdx) {
				list.array[currIdx] = list.array[endIdx];
			}
			list.array[endIdx] = null;
			list.size = endIdx;
			canRemove = false;
		}
		
	}
	
}
