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
 * @see simpledb.HeapPage#HeapPage.
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int numPages;

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
        this.numPages = ((int) f.length() + pageSize - 1) / pageSize;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
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
        byte[] buffer = new byte[BufferPool.getPageSize()];

        try {
            reader = new RandomAccessFile(f, "r");
            reader.seek(pid.pageNumber() * BufferPool.getPageSize());
            reader.read(buffer);
            reader.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), buffer);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // See DbFile.java for javadocs.
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    // See DbFile.java for javadocs.
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        return null;
    }

    // See DbFile.java for javadocs.
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        return null;
    }

    // See DbFile.java for javadocs.
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
}
