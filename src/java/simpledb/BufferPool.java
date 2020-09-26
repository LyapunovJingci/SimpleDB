package simpledb;

import java.util.*;
import java.io.*;
import java.math.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    public static final int PAGE_SIZE_PUB = PAGE_SIZE;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private int pageNum;
    private HashMap<PageId, Page>  pid2page;
    //private HashMap<Integer, Integer>  tid2pid;
    //private HashMap<Integer, Integer>  pid2tid;
    //private int time;
    //private int dtime;
    public BufferPool(int numPages) {
        // some code goes here
        //需要tid哈希表吗，transaction应该是读或写吧，这个做成哈希有啥用呢?

        pageNum = numPages;
        pid2page = new HashMap<>(pageNum);
        //tid2pid = new HashMap<>(pageNum);
        //pid2tid = new HashMap<>(pageNum);
        //time = 0;
        //dtime = 0;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // perm no use for lab1? tid usage need upgrade:(
        // I am noob
        //if (pid2tid.containsKey(pid)) new throw TransactionAbortedException();
        //delete one?
        //lab1的要求应该是如果访问量大于pagesize就throw error, 然后之后的lab会写更新的rule
        //如果buffer里有就直接返回
        //如果buffer里没有，加入buffer中然后返回
        //所以这里大概是这样? (这里我没加tid相关的信息)
        ///////////////////////////////////////////////////////////
        //if (pid2page.size() > pageNum) throw new DbException();
        if (!pid2page.containsKey(pid)) {
            return pid2page.get(pid);
        }
        else {
            //这里要从一个地方读文件大概，但是没想明白从哪读，我还得再读一下其他class的代码
            // DB => Catalog + pid => table id => DBfile => page，用pid读出file然后再在file里用pid找page...
            //DbFile getDatabaseFile(int tableid)
            //Page readPage(PageId id);
            DbFile hf = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = hf.readPage(pid);
            pid2page.put(pid,page);
            return page;
        }
        //throw new DbException("not exist");
    }
        /**
         * Releases the lock on a page.
         * Calling this is very risky, and may result in wrong behavior. Think hard
         * about who needs to call this and why, and why they can run the risk of
         * calling it.
         *
         * @param tid the ID of the transaction requesting the unlock
         * @param pid the ID of the page to unlock
         */
        public  void releasePage(TransactionId tid, PageId pid) {
            // some code goes here
            // not necessary for lab1|lab2
        }

        /**
         * Release all locks associated with a given transaction.
         *
         * @param tid the ID of the transaction requesting the unlock
         */
        public void transactionComplete(TransactionId tid) throws IOException {
            // some code goes here
            // not necessary for lab1|lab2
        }

        /** Return true if the specified transaction has a lock on the specified page */
        public boolean holdsLock(TransactionId tid, PageId p) {
            // some code goes here
            // not necessary for lab1|lab2
            return false;
        }

        /**
         * Commit or abort a given transaction; release all locks associated to
         * the transaction.
         *
         * @param tid the ID of the transaction requesting the unlock
         * @param commit a flag indicating whether we should commit or abort
         */
        public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
            // some code goes here
            // not necessary for lab1|lab2
        }

        /**
         * Add a tuple to the specified table on behalf of transaction tid.  Will
         * acquire a write lock on the page the tuple is added to and any other
         * pages that are updated (Lock acquisition is not needed for lab2).
         * May block if the lock(s) cannot be acquired.
         *
         * Marks any pages that were dirtied by the operation as dirty by calling
         * their markDirty bit, and adds versions of any pages that have
         * been dirtied to the cache (replacing any existing versions of those pages) so
         * that future requests see up-to-date pages.
         *
         * @param tid the transaction adding the tuple
         * @param tableId the table to add the tuple to
         * @param t the tuple to add
         */
        public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
            // some code goes here
            // not necessary for lab1
        }

        /**
         * Remove the specified tuple from the buffer pool.
         * Will acquire a write lock on the page the tuple is removed from and any
         * other pages that are updated. May block if the lock(s) cannot be acquired.
         *
         * Marks any pages that were dirtied by the operation as dirty by calling
         * their markDirty bit, and adds versions of any pages that have
         * been dirtied to the cache (replacing any existing versions of those pages) so
         * that future requests see up-to-date pages.
         *
         * @param tid the transaction deleting the tuple.
         * @param t the tuple to delete
         */
        public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
            // some code goes here
            // not necessary for lab1
        }

        /**
         * Flush all dirty pages to disk.
         * NB: Be careful using this routine -- it writes dirty data to disk so will
         *     break simpledb if running in NO STEAL mode.
         */
        public synchronized void flushAllPages() throws IOException {
            // some code goes here
            // not necessary for lab1

        }

        /** Remove the specific page id from the buffer pool.
         Needed by the recovery manager to ensure that the
         buffer pool doesn't keep a rolled back page in its
         cache.

         Also used by B+ tree files to ensure that deleted pages
         are removed from the cache so they can be reused safely
         */
        public synchronized void discardPage(PageId pid) {
            // some code goes here
            // not necessary for lab1
        }

        /**
         * Flushes a certain page to disk
         * @param pid an ID indicating the page to flush
         */
        private synchronized  void flushPage(PageId pid) throws IOException {
            // some code goes here
            // not necessary for lab1
        }

        /** Write all pages of the specified transaction to disk.
         */
        public synchronized  void flushPages(TransactionId tid) throws IOException {
            // some code goes here
            // not necessary for lab1|lab2
        }

        /**
         * Discards a page from the buffer pool.
         * Flushes the page to disk to ensure dirty pages are updated on disk.
         */
        private synchronized  void evictPage() throws DbException {
            // some code goes here
            // not necessary for lab1
        }

    }
