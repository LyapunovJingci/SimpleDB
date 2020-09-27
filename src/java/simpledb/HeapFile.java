package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // Done
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // Done
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // Done
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // Done
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Done
        //throws IllegalArgumentException if the page does not exist in this file.
        // HeapPage(HeapPageId id, byte[] data)
        // RandomAccessFile can use seek() to find file, need use try? not sure feasible?
        try {
            RandomAccessFile file = new RandomAccessFile(f,"r");
            Page page = null;
            // the address of this page
            int loc = pid.pageNumber() * BufferPool.getPageSize();
            byte[] data = new byte[BufferPool.getPageSize()];
            /**
             * Returns the length of this file.
             *
             * @return     the length of this file, measured in bytes.
             * @exception  IOException  if an I/O error occurs.
             */
            file.seek(loc);
            /**
             * Reads a sub array as a sequence of bytes.
             * @param b the buffer into which the data is read.
             * @param off the start offset of the data.
             * @param len the number of bytes to read.
             * @exception IOException If an I/O error has occurred.
             */
            file.read(data,0,BufferPool.getPageSize());
            page = new HeapPage((HeapPageId) pid, data);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new NoSuchElementException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Done
        return (int)Math.ceil(f.length() / (double)BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // Done
        return new HeapFileIterator(tid, this);
    }



}

