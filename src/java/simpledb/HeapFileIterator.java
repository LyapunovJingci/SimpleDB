package simpledb;

import java.io.*;
import java.util.*;

/**
 * Returns an iterator over all the tuples stored in this DbFile. The
 * iterator must use {@link BufferPool#getPage}, rather than
 * {@link HeapFile#readPage} to iterate through the pages.
 *
 * @return an iterator over all the tuples stored in this DbFile.
 */
public class HeapFileIterator implements DbFileIterator{
    private TransactionId tid;
    private HeapFile hf;
    private Iterator<Tuple> t;
    private int index;

    /**
    * Constructs a iterator by tid and HeapFile
    */
    public HeapFileIterator(TransactionId tid, HeapFile hf){
        this.tid = tid;
        this.hf = hf;
    }

    /**
     * get Iterable<Tuple> from BufferPool heappage <==> page
     * DataBase => BufferPool(tid,pid,perm) => get page => HeapPage Iterator<Tuple> iterator()
     * @return Iterable<Tuple>
     */
    public Iterator<Tuple> getT(HeapPageId pid) throws TransactionAbortedException, DbException {
        Permissions perm = Permissions.READ_WRITE;
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, perm);
        return hp.iterator();
    }

    /**
     * open iterator and index => first page
     */
    @Override
    public void open() throws TransactionAbortedException, DbException {
        index = 0;
        HeapPageId pid = new HeapPageId(hf.getId(), index);
        t = getT(pid);
    }

    /**
     * check null, hasNext for this page, hasNext for whole HeapFile(next not empty)
     * @return hasNext
     */
    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(t == null) return false;
        if(t.hasNext()) return true;
        if(hf.numPages() > index + 1){
            index++;
            return getT(new HeapPageId(hf.getId(), index)).hasNext();
        }
        return false;
    }
    @Override
    public Tuple next() throws TransactionAbortedException, DbException {
        if(!hasNext()) throw new NoSuchElementException();
        else{
            return t.next();
        }
    }

    /**
     * need to close it at first
     * @throws TransactionAbortedException
     * @throws DbException
     */
    @Override
    public void rewind() throws TransactionAbortedException, DbException {
        close();
        open();
    }
    @Override
    public void close(){
        t = null;
    }
}
