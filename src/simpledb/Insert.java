package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
	private TransactionId tid = null;
	private DbIterator child = null;
	private int tableId = -1;
	
	// 用来记录readNext的调用次数，且判断readnext是不是已经被调用过
	private int insert_readNextTimes = -1;
	
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	this.tableId = tableid;
    	if (!Database.getCatalog().getTupleDesc(this.tableId).equals(this.getTupleDesc()))
    	{
    		throw new DbException("Insert.java Insert: TupleDesc of child differs from table into which we are to insert!");
    	}
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return this.child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.open();
    }

    public void close() {
        // some code goes here
    	this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        // some code goes here
    	BufferPool bufferPool = Database.getBufferPool();
    	int count = 0;
    	while (child.hasNext())
    	{
    		try {
				bufferPool.insertTuple(this.tid, this.tableId, child.next());
			} catch (NoSuchElementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		count++;
    	}
    	// if called more than once
    	if (count == 0 && this.insert_readNextTimes != -1)
    	{
    		return null;
    	}
    	this.insert_readNextTimes = count;
    	// 返回Tuple
    	Type[] typeAr = new Type[1];
    	typeAr[0] = Type.INT_TYPE;
    	TupleDesc td = new TupleDesc(typeAr);
    	Tuple tuple = new Tuple(td);
    	IntField f = new IntField(this.insert_readNextTimes);
    	tuple.setField(0, f);
        return tuple;
    }
}
