package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {
	private PageId pageId = null;
	private int tupleNumber = 0;
	
    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	this.pageId = pid;
    	this.tupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return this.tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	// some code goes here
//    	throw new UnsupportedOperationException("implement this");
    	if (o instanceof RecordId)
    	{
    		RecordId temp = (RecordId)o;
    		if (temp.pageId.equals(this.pageId) && temp.tupleNumber == this.tupleNumber)
    		{
    			return true;
    		}
    		return false;
    	}
    	throw new UnsupportedOperationException("Input RecordId");
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	// some code goes here
//    	throw new UnsupportedOperationException("implement this");
    	int hash = 17;
    	hash = hash * 31 + this.pageId.hashCode();
    	hash = hash * 31 + this.tupleNumber;
    	return hash;
    }
}
