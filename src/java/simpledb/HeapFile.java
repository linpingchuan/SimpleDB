package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage.
 */
public class HeapFile implements DbFile {

  private final File f;
  private final TupleDesc td;
  private final AtomicInteger numPages;

  /**
   * Constructs a heap file backed by the specified file.
   * 
   * @param f The file that stores the on-disk backing store for this heap
   * file.
   */
  public HeapFile(File f, TupleDesc td) {
    final int pageSize = BufferPool.getPageSize();

    this.f = f;
    this.td = td;
    this.numPages = new AtomicInteger(((int) f.length() + pageSize - 1) / pageSize);
  }

  /**
   * Returns the File backing this HeapFile on disk.
   */
  public File getFile() {
    return f;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile (a.k.a. tableId).
   *
   * Implementation note: you will need to generate this tableid somewhere
   * ensure that each HeapFile has a "unique id," and that you always return
   * the same value for a particular HeapFile. We suggest hashing the absolute
   * file name of the file underlying the heapfile, i.e.
   * f.getAbsoluteFile().hashCode().
   * 
   * @return An ID uniquely identifying this HeapFile.
   */
  public int getId() {
    return f.getAbsoluteFile().hashCode();
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   */
  public TupleDesc getTupleDesc() {
    return td;
  }

  // See DbFile.java for javadocs.
  public Page readPage(PageId pid) throws IllegalArgumentException {
    RandomAccessFile reader = null;
    byte[] buffer = new byte[BufferPool.getPageSize()]; // all 0

    try {
      int from = pid.pageNumber() * BufferPool.getPageSize();

      reader = new RandomAccessFile(f, "r");
      if (from < reader.length()) {
        reader.seek(from);
        reader.read(buffer);
        reader.close();
      }
      // If the page to read exceeds file length, allocate a new empty page.
      return new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), buffer);
    } catch (IOException e) {
      throw new IllegalArgumentException();
    }
  }

  // See DbFile.java for javadocs.
  public void writePage(Page page) throws IOException {
    RandomAccessFile writer = null;
    int from = page.getId().pageNumber() & BufferPool.getPageSize();

    writer = new RandomAccessFile(f, "rw");
    writer.seek(from);
    writer.write(page.getPageData());
    writer.close();
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    return numPages.get();
  }

  // See DbFile.java for javadocs.
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    int tableId = getId();
    ArrayList<Page> ret = new ArrayList<Page>();

    for (int i = 0; i < numPages.get(); ++i) {
      HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
          new HeapPageId(tableId, i), Permissions.READ_WRITE);

      if (page.getNumEmptySlots() > 0) {
        page.insertTuple(t);
        ret.add(page);
        return ret;
      }
    }

    // No slots for any page, try to allocate a new one.
    int newPageIndex = numPages.getAndIncrement();
    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
        new HeapPageId(tableId, newPageIndex), Permissions.READ_WRITE);
    page.insertTuple(t);
    ret.add(page);

    return ret;
  }

  // See DbFile.java for javadocs.
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    ArrayList<Page> ret = new ArrayList<Page>();
    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
        t.getRecordId().getPageId(), Permissions.READ_WRITE);

    page.deleteTuple(t);
    ret.add(page);

    return ret;
  }

  // See DbFile.java for javadocs.
  public DbFileIterator iterator(TransactionId tid) {
    return new HeapFileIterator(tid, this);
  }
}
