/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.util;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * Implementation of {@link java.util.List} where all methods throw
 * an UnsupportedOperationException exception except for the
 * <code>isEmpty</code> method. The <code>iterator</code> and
 * <code>listIterator</code> methods can be easily implemented in
 * derived classes by using the helper inner classes:
 * <code>Itr</code> and <code>ListItr</code>.
 * These iterators are all read only,
 * their <code>remove</code>, <code>add</code> and <code>set</code>
 * methods throw the
 * UnsupportedOperationException exception.
 * <p>
 * This class can be used for List implementations that only implement
 * a subset of all the methods.
 *
 * @author Richard Emberson
 * @since Jan 16, 2007
 */
public abstract class UnsupportedList<T> implements List<T> {
    private static final Logger LOGGER =
        LogManager.getLogger(UnsupportedList.class);

    protected UnsupportedList() {
    }

    @Override
    public boolean isEmpty() {
        return (size() == 0);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException(getClass().getName() + ".size");
    }

    @Override
    public T get(int index) {
        throw new UnsupportedOperationException(getClass().getName() + ".get");
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException(getClass().getName() + ".set");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException(
            getClass().getName() + ".toArray");
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".add");
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".remove");
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".indexOf");
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".lastIndexOf");
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".subList");
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".contains");
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".toArray");
    }

    @Override
    public boolean add(T o) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".add");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".remove");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".containsAll");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".addAll");
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".addAll");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".removeAll");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".retainAll");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(
            getClass().getName() + ".clear");
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".equals");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
            getClass().getName() + ".hashCode");
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException(
            getClass().getName() + ".listIterator");
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException(
            getClass().getName() + ".listIterator");
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException(
            getClass().getName() + ".iterator");
    }



    protected class Itr implements Iterator<T> {
        protected int cursor;
        protected int lastRet;

        public Itr() {
            this.cursor = 0;
            this.lastRet = -1;
        }

        @Override
        public boolean hasNext() {
            return (cursor != size());
        }

        @Override
        public T next() {
            try {
                T next = get(cursor);
                lastRet = cursor++;
                return next;
            } catch (IndexOutOfBoundsException e) {
                LOGGER.error(
                    "UnsupportedList.Itr.next: cursor="
                        +  cursor
                        + ", size="
                        + size(), e);
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(
                getClass().getName() + ".remove");
        }
    }

    protected class ListItr extends Itr implements ListIterator<T> {
        public ListItr(int index) {
            this.cursor = index;
        }

        @Override
        public boolean hasPrevious() {
            return cursor != 0;
        }

        @Override
        public T previous() {
            try {
                int i = cursor - 1;
                T previous = get(i);
                lastRet = cursor = i;
                return previous;
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public int previousIndex() {
            return cursor - 1;
        }

        @Override
        public void set(T o) {
/*
            if (lastRet == -1)
                throw new IllegalStateException();
            try {
                MemberList.this.set(lastRet, o);
            } catch (IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
*/
            throw new UnsupportedOperationException(
                getClass().getName() + ".set");
        }

        @Override
        public void add(T o) {
            throw new UnsupportedOperationException(
                getClass().getName() + ".add");
        }
    }

    /**
     * Iterator for arrays of a priori unknown size.
     */
    protected class ItrUnknownSize extends Itr {
        public ItrUnknownSize() {
            super();
        }

        @Override
        public boolean hasNext() {
            try {
                get(cursor);
                return true;
            } catch (IndexOutOfBoundsException e) {
                return false;
            }
        }
    }
}

// End UnsupportedList.java

