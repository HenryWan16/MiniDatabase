package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
	private TransactionId tid = null;
	private DbIterator child = null;
	
	// 用来记录调用readNext()的次数
	private int delete_readNextTimes = -1;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.child = child;
    	this.tid = t;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	BufferPool bufferPool = Database.getBufferPool();
    	int count = 0;
    	while (child.hasNext())
    	{
			bufferPool.deleteTuple(this.tid, child.next());
    		count++;
    	}
    	if (count == 0 && this.delete_readNextTimes != -1)
    	{
    		return null;
    	}
    	this.delete_readNextTimes = count;
    	// 返回Tuple
    	Type[] typeAr = new Type[1];
    	typeAr[0] = Type.INT_TYPE;
    	TupleDesc td = new TupleDesc(typeAr);
    	Tuple tuple = new Tuple(td);
    	IntField f = new IntField(this.delete_readNextTimes);
    	tuple.setField(0, f);
        return tuple;
    }
}
