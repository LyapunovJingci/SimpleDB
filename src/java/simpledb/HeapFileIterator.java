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
    public Iterator<Tuple> getI(int id) throws TransactionAbortedException, DbException {
        Permissions perm = Permissions.READ_WRITE;
        PageId pageId = new HeapPageId(hf.getId(), id);
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pageId, perm);
        //System.out.format("not null\n");
        return hp.iterator();
    }


    /**
     * open iterator and index => first page
     */
    @Override
    public void open() throws TransactionAbortedException, DbException {
        index = 0;
        t = getI(index);
    }

    /**
     * check null, hasNext for this page, hasNext for whole HeapFile(next not empty)
     * @return hasNext
     */
    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(t == null){
            //System.out.format("null\n");
            return false;
        }
        else if(t.hasNext()) {
            return true;
        }
        else if(hf.numPages() > index+1){
            //index++;
            //System.out.format("not null\n");
            //System.out.println(getT(new HeapPageId(hf.getId(), index)).hasNext());
            return getI(index+1).hasNext();
        }
        //System.out.format("%d %d\n",hf.numPages(),index+1);
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
        //System.out.println(hasNext());
        if(t == null) throw new NoSuchElementException("null");
        else if(t.hasNext()) {
            return t.next();
        }
        else{
            if(hf.numPages() > index + 1){
                index++;
                t = null;
                t = getI(index);
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
    @Override
    public void close(){
        t = null;
    }
}
