package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pid;
    private final int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid The pageid of the page on which the tuple resides.
     *
     * @param tupleno The tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * Returns the tuple number this RecordId references.
     */
    public int tupleno() {
        return tupleno;
    }

    /**
     * Returns the page id this RecordId references.
     */
    public PageId getPageId() {
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple.
     */
    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof RecordId) {
            RecordId id = (RecordId) o;

            return id.pid.equals(pid) && id.tupleno == tupleno;
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return pid.hashCode() * 233 + tupleno;
    }
}
