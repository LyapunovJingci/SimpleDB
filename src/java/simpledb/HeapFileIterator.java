package simpledb;

import java.util.*;

/**
 * A helper class for iterator class HeapFile class.
 * This class implements the DbFileIterator interface
 */
public class HeapFileIterator implements DbFileIterator{
    private TransactionId tid;
    private HeapFile hf;
    private Iterator<Tuple> iterator;
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
    public Iterator<Tuple> getIterator(int id) throws TransactionAbortedException, DbException {
        Permissions perm = Permissions.READ_ONLY;
        PageId pageId = new HeapPageId(hf.getId(), id);
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pageId, perm);
        return hp.iterator();
    }

    /**
     * Opens the iterator, index = 0, starting from the first page
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws TransactionAbortedException, DbException {
        index = 0;
        iterator = getIterator(index);
    }

    /**
     * check null, hasNext for this page, hasNext for whole HeapFile(next not empty)
     * @return hasNext
     */
    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(iterator == null){
            return false;
        }
        if (hf.numPages() > index + 1) {
            return getIterator(index + 1).hasNext();
        } else {
            return iterator.hasNext();
        }

    }

    /**
     * check t == null => has next in this page => has next page => has tuple in next page
     * @return the next Tuple
     * @throws TransactionAbortedException
     * @throws DbException
     */
    @Override
    public Tuple next() throws TransactionAbortedException, DbException {
        if (iterator == null) throw new NoSuchElementException("null");
        if (iterator.hasNext()) {
            return iterator.next();
        }

        if (hf.numPages() > index + 1) {
            index++;
            iterator = getIterator(index);
            if(iterator.hasNext()) {
                return iterator.next();
            }
        }
        throw new NoSuchElementException("end");
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws TransactionAbortedException, DbException {
        close();
        open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close(){
        iterator = null;
    }
}
