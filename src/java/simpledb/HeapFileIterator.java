package simpledb;

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
    public Iterator<Tuple> getIterator(int id) throws TransactionAbortedException, DbException {
        Permissions perm = Permissions.READ_ONLY;
        PageId pageId = new HeapPageId(hf.getId(), id);
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pageId, perm);
        return hp.iterator();
    }
    
    /**
     * open iterator and index => first page
     */
    @Override
    public void open() throws TransactionAbortedException, DbException {
        index = 0;
        t = getIterator(index);
    }

    /**
     * check null, hasNext for this page, hasNext for whole HeapFile(next not empty)
     * @return hasNext
     */
    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(t == null){
            return false;
        }
        else if(t.hasNext()) {
            return true;
        }
        else if(hf.numPages() > index+1){
            return getIterator(index+1).hasNext();
        }
        return false;
    }

    /**
     * check t == null => has next in this page => has next page => has tuple in next page
     * @return the next Tuple
     * @throws TransactionAbortedException
     * @throws DbException
     */
    @Override
    public Tuple next() throws TransactionAbortedException, DbException {
        if(t == null) throw new NoSuchElementException("null");
        else if(t.hasNext()) {
            return t.next();
        }
        else{
            if(hf.numPages() > index + 1){
                index++;
                t = null;
                t = getIterator(index);
                if(t.hasNext()) return t.next();
                else{
                    throw new NoSuchElementException("end");
                }
            }
            else{
                throw new NoSuchElementException("end");
            }
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

    /**
     * clear the iterator to null
     */
    @Override
    public void close(){
        t = null;
    }
}
