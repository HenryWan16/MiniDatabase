package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {
	private Aggregator aggregator = null;
	private DbIterator child = null;
	private int gbfield = 0;
	private int afield = 0;
	private Aggregator.Op aop = null;
	
	private DbIterator it = null;
    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
    	this.child = child;
    	this.gbfield = gfield;
    	this.afield = afield;
    	this.aop = aop;
    	
    	Type gbfieldtype = null;
    	TupleDesc tempTupleDesc = child.getTupleDesc();
    	// 如果聚集函数作用的是整型域
    	if (tempTupleDesc.getType(afield) == Type.INT_TYPE)
    	{
    		// GroupBy的情况
    		if (this.gbfield != Aggregator.NO_GROUPING)
    		{
    			gbfieldtype = tempTupleDesc.getType(gfield);
    		}
    		this.aggregator = new IntAggregator(gfield, gbfieldtype, afield, aop);	
    	}
    	else if (tempTupleDesc.getType(afield) == Type.STRING_TYPE)
    	{
    		// GroupBy的情况
    		if (this.gbfield != Aggregator.NO_GROUPING)
    		{
    			gbfieldtype = tempTupleDesc.getType(gfield);
    		}
    		this.aggregator = new StringAggregator(gfield, gbfieldtype, afield, aop);
    	}
    	else
    	{
    		System.out.println("Error: gbfield is neither INT_TYPE nor STRING_TYPE!");
    	}
    	
    	// merge操作完成了Aggregate里面的ArrayList属性
    	try {
			while (child.hasNext())
			{
				this.aggregator.merge(child.next());
			}
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	// 构造的时候自动open()了iterator
    	this.it = this.aggregator.iterator();
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
    	this.it.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	Tuple resultTuple = null;
    	if (this.it.hasNext())
    	{
    		resultTuple = this.it.next();
    	}
    	else
    	{
    		return null;
    	}
	    if (this.gbfield == Aggregator.NO_GROUPING)
	    {
	    	
	    }
	    else
	    {
	    	
	    }
        return resultTuple;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc tempDesc = this.child.getTupleDesc();
    	Type tempAfieldType = Type.INT_TYPE;
//    	String tempAfieldName = Aggregate.aggName(this.aop) + "(" + tempDesc.getFieldName(this.afield) + ")";
    	String tempAfieldName = tempDesc.getFieldName(this.afield);
    	TupleDesc tempTupleDesc = null;
    	if (this.gbfield == Aggregator.NO_GROUPING)
    	{
    		Type[] typeAr = new Type[1];
    		typeAr[0] = tempAfieldType;
    		String[] fieldAr = new String[1];
    		fieldAr[0] = tempAfieldName;
    		tempTupleDesc = new TupleDesc(typeAr, fieldAr);
    	}
    	else
    	{
//    		String tempGbfieldName = Aggregate.aggName(this.aop) + "(" + tempDesc.getFieldName(this.gbfield) + ")";
    		String tempGbfieldName = tempDesc.getFieldName(this.gbfield);
    		Type tempGbfieldType = tempDesc.getType(this.gbfield);
    		Type[] typeAr = new Type[2];
    		typeAr[0] = tempGbfieldType;
    		typeAr[1] = tempAfieldType;
    		String[] fieldAr = new String[2];
    		fieldAr[0] = tempGbfieldName;
    		fieldAr[1] = tempAfieldName;
    		tempTupleDesc = new TupleDesc(typeAr, fieldAr);
    	}
        return tempTupleDesc;
    }

    public void close() {
        // some code goes here
    	this.it.close();
    }
}
