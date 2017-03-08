package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class HeapFileReadTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After
    public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.getId()
     */
    @Test
    public void getId() throws Exception {
        int id = hf.getId();

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, hf.getId());
        assertEquals(id, hf.getId());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.getId());
    }

    /**
     * Unit test for HeapFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() throws Exception {    	
        assertEquals(td, hf.getTupleDesc());        
    }
    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() throws Exception {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
    }

    /**
     * Unit test for HeapFile.readPage()
     */
    @Test
    public void readPage() throws Exception {
        HeapPageId pid = new HeapPageId(hf.getId(), 0);
        HeapPage page = (HeapPage) hf.readPage(pid);

        // NOTE(ghuo): we try not to dig too deeply into the Page API here; we
        // rely on HeapPageTest for that. perform some basic checks.
        assertEquals(484, page.getNumEmptySlots());
        assertTrue(page.isSlotUsed(1));
        assertFalse(page.isSlotUsed(20));
    }
    
    @Test
    public void readFromFileNotMemoryTest() throws Exception {
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>(10);
        for (int i = 0; i < 10; ++i) {
            ArrayList<Integer> tuple = new ArrayList<Integer>(2);
            for (int j = 0; j < 2; ++j) {
                tuple.add(0);
            }
            tuples.add(tuple);
        }
        HeapFileEncoder.convert(tuples, hf.getFile(), BufferPool.PAGE_SIZE, 2);
        HeapPageId pid = new HeapPageId(hf.getId(), 0);
        HeapPage page = (HeapPage) hf.readPage(pid);
        
        tuples.clear();
        for (int i = 0; i < 10; ++i) {
            ArrayList<Integer> tuple = new ArrayList<Integer>(2);
            for (int j = 0; j < 2; ++j) {
                tuple.add(1);
            }
            tuples.add(tuple);
        }
        HeapFileEncoder.convert(tuples, hf.getFile(), BufferPool.PAGE_SIZE, 2);
        HeapPageId pid1 = new HeapPageId(hf.getId(), 0);
        HeapPage page1 = (HeapPage) hf.readPage(pid1);
        
        Iterator<Tuple> it = page.iterator();
        Iterator<Tuple> it1 = page1.iterator();
        while (it.hasNext()) {
            Tuple tup = it.next();
            Tuple tup1 = it1.next();
            assertTrue(!tup.toString().equals(tup1.toString()));
        }
    }
    
    @Test
    public void readTwoPages() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 2000, null, null);
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>(10);
        tuples.clear();
        for (int i = 0; i < 2000; ++i) {
            ArrayList<Integer> tuple = new ArrayList<Integer>(2);
            for (int j = 0; j < 2; ++j) {
            	if (i == 0)
            		tuple.add(0);
            	else
            		tuple.add(1);
            }
            tuples.add(tuple);
        }
        HeapFileEncoder.convert(tuples, hf.getFile(), BufferPool.PAGE_SIZE, 2);
        
        HeapPageId pid0 = new HeapPageId(hf.getId(), 0);
        HeapPage page0 = (HeapPage) hf.readPage(pid0);
        Iterator<Tuple> it0 = page0.iterator();
        Tuple tup0 = it0.next();
        assertTrue(tup0.getField(0).toString().equals("0"));
        
        HeapPageId pid1 = new HeapPageId(hf.getId(), 1);
        HeapPage page1 = (HeapPage) hf.readPage(pid1);
        Iterator<Tuple> it1 = page1.iterator();
        Tuple tup1 = it1.next();
        assertTrue(tup1.getField(0).toString().equals("1"));
    }
     
    @Test
    public void testIteratorBasic() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);

        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }

        it.open();
        int count = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            count += 1;
        }
        assertEquals(3, count);
        it.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520,
                null, null);

        DbFileIterator it = twoPageFile.iterator(tid);
        it.open();
        assertTrue(it.hasNext());
        it.close();
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }
        // close twice is harmless
        it.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
