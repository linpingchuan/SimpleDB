package simpledb;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 *
 * The BufferPool is also responsible for locking. When a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final.
 */
public class BufferPool {
  /** Bytes per page, including header. */
  public static final int PAGE_SIZE = 4096;

  // TODO(foreverbell): Why this variable is unused?
  private static int pageSize = PAGE_SIZE;

  /**
   * Default number of pages passed to the constructor. This is used by
   * other classes. BufferPool should use the numPages argument to the
   * constructor instead. */
  public static final int DEFAULT_PAGES = 50;

  private final int numPages;
  private final ConcurrentHashMap<PageId, Page> pool;
  private final LockManager lockman;

  /**
   * Creates a BufferPool that caches up to numPages pages.
   *
   * @param numPages Maximum number of pages in this buffer pool.
   */
  public BufferPool(int numPages) {
    this.numPages = numPages;
    this.pool = new ConcurrentHashMap<PageId, Page>();
    this.lockman = new LockManager();
  }
  
  public static int getPageSize() {
    return PAGE_SIZE;
  }

  // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
  public static void setPageSize(int pageSize) {
    BufferPool.pageSize = pageSize;
  }

  /**
   * Retrieve the specified page with the associated permissions.
   *
   * Will acquire a lock and may block if that lock is held by another
   * transaction.
   *
   * The retrieved page should be looked up in the buffer pool.  If it
   * is present, it should be returned.  If it is not present, it should
   * be added to the buffer pool and returned.  If there is insufficient
   * space in the buffer pool, an page should be evicted and the new page
   * should be added in its place.
   *
   * @param tid The ID of the transaction requesting the page.
   *
   * @param pid The ID of the requested page.
   *
   * @param perm The requested permissions on the page.
   */
  public Page getPage(TransactionId tid, PageId pid, Permissions perm)
      throws TransactionAbortedException, DbException {
    Page page;

    // Try to acquire the lock for this transaction.
    if (perm == Permissions.READ_ONLY) {
      lockman.acquireShared(tid, pid);
    } else {
      lockman.acquireExclusive(tid, pid);
    }

    page = pool.get(pid);

    if (page != null) {
       return page;
    }

    page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    while (pool.size() >= numPages) {
      evictPage();
    }
    pool.put(pid, page);

    return page;
  }

  /**
   * Releases the lock on a page.
   *
   * Calling this is very risky, and may result in wrong behavior. Think hard
   * about who needs to call this and why, and why they can run the risk of
   * calling it.
   *
   * @param tid The ID of the transaction requesting the unlock.
   *
   * @param pid The ID of the page to unlock.
   */
  public void releasePage(TransactionId tid, PageId pid) {
    lockman.release(tid, pid);
  }

  /**
   * Release all locks associated with a given transaction.
   *
   * @param tid The ID of the transaction requesting the unlock.
   */
  public void transactionComplete(TransactionId tid) throws IOException {
    transactionComplete(tid, true);
  }

  /**
   * Returns true if the specified transaction has a lock on the specified
   * page.
   * */
  public boolean holdsLock(TransactionId tid, PageId pid) {
    return lockman.holdsLock(tid, pid);
  }

  /**
   * Commit or abort a given transaction; release all locks associated to
   * the transaction.
   *
   * @param tid The ID of the transaction requesting the unlock.
   *
   * @param commit A flag indicating whether we should commit or abort.
   */
  public void transactionComplete(TransactionId tid, boolean commit)
      throws IOException {
    Iterator<Entry<PageId, Page>> iter = pool.entrySet().iterator();

    while (iter.hasNext()) {
      Entry<PageId, Page> next = iter.next();
      PageId pid = next.getKey();
      Page p = next.getValue();

      if (tid.equals(p.isDirty())) {
        // Some tests require flushing dirty pages on transactionComplete.
        if (commit) {
          flushPage(pid);
          p.markDirty(false, null);
          // Use current page contents as the before-image for the next
          // transaction that modifies this page.
          p.setBeforeImage();
        } else { // abort
          pool.put(pid, p.getBeforeImage());
        }
      }
    }

    lockman.releaseAll(tid);
  }

  /**
   * Add a tuple to the specified table on behalf of transaction tid.
   *
   * Will acquire a write lock on the page the tuple is added to and any other 
   * pages that are updated (Lock acquisition is not needed for lab2). 
   * May block if the lock(s) cannot be acquired.
   * 
   * Marks any pages that were dirtied by the operation as dirty by calling
   * their markDirty bit, and updates cached versions of any pages that have 
   * been dirtied so that future requests see up-to-date pages. 
   *
   * @param tid The transaction adding the tuple.
   *
   * @param tableId The table to add the tuple to.
   *
   * @param t The tuple to add.
   */
  public void insertTuple(TransactionId tid, int tableId, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    ArrayList<Page> ps;

    ps = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    for (Page p : ps) {
      p.markDirty(true, tid);
      // Reinsert the dirty page into buffer pool, this is necessary if we
      // want to evict a clean page locked by a running transaction.
      pool.put(p.getId(), p);
    }
  }

  /**
   * Remove the specified tuple from the buffer pool.
   *
   * Will acquire a write lock on the page the tuple is removed from and any
   * other pages that are updated. May block if the lock(s) cannot be acquired.
   *
   * Marks any pages that were dirtied by the operation as dirty by calling
   * their markDirty bit, and updates cached versions of any pages that have 
   * been dirtied so that future requests see up-to-date pages. 
   *
   * @param tid The transaction deleting the tuple.
   *
   * @param t The tuple to delete.
   */
  public void deleteTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    int tableId = t.getRecordId().getPageId().getTableId();
    ArrayList<Page> ps;

    ps = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
    for (Page p : ps) {
      p.markDirty(true, tid);
      // Reinsert the dirty page into buffer pool, this is necessary if we
      // want to evict a clean page locked by a running transaction.
      pool.put(p.getId(), p);
    }
  }

  /**
   * Flush all dirty pages to disk.
   *
   * NB: Be careful using this routine -- it writes dirty data to disk so will
   * break simpledb if running in NO STEAL mode.
   */
  public synchronized void flushAllPages() throws IOException {
    for (Page page : pool.values()) {
      flushPage(page.getId());
    }
  }

  /**
   * Remove the specific page id from the buffer pool.
   *
   * Needed by the recovery manager to ensure that the buffer pool doesn't
   * keep a rolled back page in its cache.
   */
  public synchronized void discardPage(PageId pid) {
    pool.remove(pid);
  }

  /**
   * Flushes a certain page to disk.
   *
   * @param pid An ID indicating the page to flush.
   */
  private synchronized void flushPage(PageId pid) throws IOException {
    Page page = pool.get(pid);

    if (page == null) {
      throw new IOException("page not in buffer pool.");
    }

    // Append an update record to the log, with a before-image and after-image.
    TransactionId dirtier = page.isDirty();
    if (dirtier != null) {
      Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
      Database.getLogFile().force();
    }

    Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);

    // Intended being dirty, waiting to be reapped by upstream calls.
    // page.markDirty(false, null);
  }

  /** Write all pages of the specified transaction to disk. */
  public synchronized void flushPages(TransactionId tid) throws IOException {
    Iterator<Entry<PageId, Page>> iter = pool.entrySet().iterator();

    while (iter.hasNext()) {
      Entry<PageId, Page> next = iter.next();
      PageId pid = next.getKey();
      Page p = next.getValue();

      if (tid.equals(p.isDirty())) {
        flushPage(pid);
        p.markDirty(false, null);
        // Use current page contents as the before-image for the next
        // transaction that modifies this page.
        p.setBeforeImage();
      }
    }
  }

  /**
   * Discards a page from the buffer pool.
   *
   * Flushes the page to disk to ensure dirty pages are updated on disk.
   */
  private synchronized void evictPage() throws DbException {
    Iterator<Entry<PageId, Page>> iter = pool.entrySet().iterator();

    while (pool.size() >= numPages && iter.hasNext()) {
      Entry<PageId, Page> next = iter.next();
      PageId pid = next.getKey();
      Page p = next.getValue();

      // NO STEAL policy implies we should never evict dirty pages.
      if (p.isDirty() != null) {
        continue;
      }
      pool.remove(pid);
    }

    if (pool.size() >= numPages) {
      throw new DbException("running out buffer pool.");
    }
  }
}
