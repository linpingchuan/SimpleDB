package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
  private final TransactionId tid;
  private final HeapFile hf;

  private int pageNo = 0;
  private Iterator<Tuple> tupleIter = null;

  HeapFileIterator(TransactionId tid, HeapFile hf) {
    this.tid = tid;
    this.hf = hf;
  }

  /**
   * Opens the iterator.
   *
   * @throws DbException When there are problems opening/accessing the
   * database.
   */
  public void open() throws DbException, TransactionAbortedException {
    pageNo = 0;
    tupleIter = openPage(pageNo).iterator();
  }

  private HeapPage openPage(int pageNo) 
      throws DbException, TransactionAbortedException {
    if (pageNo < 0 || pageNo >= hf.numPages()) {
      return null;
    }
    // TODO(foreverbell): Permissions.READ_ONLY is okay?
    return (HeapPage) Database.getBufferPool()
      .getPage(tid, new HeapPageId(hf.getId(), pageNo), Permissions.READ_ONLY);
  }

  /** Returns true if there are more tuples available. */
  public boolean hasNext() throws DbException, TransactionAbortedException {
    // Not opened or reached EOF.
    if (tupleIter == null || pageNo >= hf.numPages()) {
      return false;
    }
    while (!tupleIter.hasNext()) {
      pageNo += 1;
      if (pageNo >= hf.numPages()) {
        return false;
      }
      tupleIter = openPage(pageNo).iterator();
    }
    return true;
  }

  /**
   * Gets the next tuple from the operator (typically implementing by reading
   * from a child operator or an access method).
   *
   * @return The next tuple in the iterator.
   *
   * @throws NoSuchElementException If there are no more tuples
   */
  public Tuple next() throws DbException, TransactionAbortedException,
      NoSuchElementException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return tupleIter.next();
  }

  /** Resets the iterator to the start. */
  public void rewind() throws DbException, TransactionAbortedException {
    open();
  }

  /** Closes the iterator. */
  public void close() {
    pageNo = 0;
    tupleIter = null;
  }
}
