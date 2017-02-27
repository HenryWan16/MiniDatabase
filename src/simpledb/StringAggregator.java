package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private Op op = null;
	private int gbfield = 0;
	private Type gbfieldtype = null;
	private ArrayList<Tuple> list = new ArrayList<Tuple>();
	private TupleDesc tupleDesc = null;
	private StringAggregatorIterator stringAggregatorIterator = null;
	// 仅仅用于记录No_GroupBy时候的情况
	private int count = 0;
	// 用于记录group_by的情况
	private HashMap<String, Integer> countHash = new HashMap<String, Integer>();
	private HashMap<Integer, Integer> countHash_Int = new HashMap<Integer, Integer>();
	
	private class StringAggregatorIterator implements DbIterator
	{
		Iterator<Tuple> it = null;
		
		public StringAggregatorIterator()
		{
			try {
				this.open();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public StringAggregatorIterator(ArrayList<Tuple> l)
		{
			this.it = l.iterator();
		}
		
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			this.it = list.iterator();
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			if (it == null)
			{
				return false;
			}
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			// TODO Auto-generated method stub
			if (it == null)
			{
				throw new NoSuchElementException("tuple is null (StringAggregator)");
			}
			return it.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			this.close();
			this.open();
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return tupleDesc;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			this.it = null;
		}
		
	}
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.op = what;
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	if (what != Op.COUNT)
    	{
    		throw new IllegalArgumentException();
    	}
    	// 遇到No_Grouping，初始化为(aggregateValue)，而此时类型仅仅为int，即Type.INT_TYPE
    	if (gbfieldtype == null)
    	{
    		Type[] tempType = new Type[1];
    		tempType[0] = Type.STRING_TYPE;
    		this.tupleDesc = new TupleDesc(tempType);
    	}
    	else
    	{
    		Type[] tempType = new Type[2];
    		if (gbfieldtype == Type.STRING_TYPE)
    		{
    			tempType[0] = Type.STRING_TYPE;
    		}
    		else
    		{
    			tempType[0] = Type.INT_TYPE;
    		}
    		tempType[1] = Type.STRING_TYPE;
    		this.tupleDesc = new TupleDesc(tempType);
    	}
    	this.stringAggregatorIterator = new StringAggregatorIterator(this.list);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	// 是表中的第一个元素吗?
    	if (this.list.size() == 0)
    	{
    		// No_GroupBy
    		if (this.tupleDesc.numFields() == 1)
    		{
    			this.count = 1;
    			Type[] tempType = new Type[1];
        		tempType[0] = Type.INT_TYPE;
        		TupleDesc tempTupleDesc = new TupleDesc(tempType);
    			Tuple tempTuple = new Tuple(tempTupleDesc);
    			IntField f = new IntField(count);
    			tempTuple.setField(0, f);
    			this.list.add(tempTuple);
    		}
    		// GroupBy
    		else
    		{
    			// 先设置HashMap
    			Field tempField = tup.getField(this.gbfield);
    			if (tempField instanceof IntField)
    			{
    				IntField tempGbField = (IntField)tup.getField(this.gbfield);
    				Integer tempIntegerGbField = tempGbField.getValue();
    				this.countHash_Int.put(tempIntegerGbField, 1);
        			Type[] tempType = new Type[2];
        			tempType[0] = Type.INT_TYPE;
            		tempType[1] = Type.INT_TYPE;
            		TupleDesc tempTupleDesc = new TupleDesc(tempType);
        			Tuple tempTuple = new Tuple(tempTupleDesc);
        			IntField f = new IntField(1);
        			tempTuple.setField(0, tempGbField);
        			tempTuple.setField(1, f);
        			// 再加入到merge结果的新表中
        			this.list.add(tempTuple);
    			}
    			else
    			{
    				StringField tempGbField = (StringField)tup.getField(this.gbfield);
    				String tempStringGbField = tempGbField.getValue();
        			this.countHash.put(tempStringGbField, 1);
        			Type[] tempType = new Type[2];
        			tempType[0] = Type.STRING_TYPE;
            		tempType[1] = Type.INT_TYPE;
            		TupleDesc tempTupleDesc = new TupleDesc(tempType);
        			Tuple tempTuple = new Tuple(tempTupleDesc);
        			IntField f = new IntField(1);
        			tempTuple.setField(0, tempGbField);
        			tempTuple.setField(1, f);
        			// 再加入到merge结果的新表中
        			this.list.add(tempTuple);
    			}
    			
    		}
    	}
    	// 表中已经有元素了
    	else
    	{
    		// No_GroupBy
    		if (this.tupleDesc.numFields() == 1)
    		{
    			this.count++;
    			Tuple tempTuple = this.list.get(0);
    			IntField f = new IntField(count);
    			tempTuple.setField(0, f);
    		}
    		// GroupBy
    		else
    		{
    			// 根据gbfield去查找HashMap，没找到则插入新的记录，找到则修改原有记录(Count++)
    			Field tempField = tup.getField(this.gbfield);
    			if (tempField instanceof IntField)
    			{
    				IntField tempGbField = (IntField)tempField;
        			Integer key = tempGbField.getValue();
        			Tuple tempTuple = null;
        			if (this.countHash_Int.containsKey(key))
        			{
        				int count = this.countHash_Int.get(key) + 1;
        				this.countHash_Int.put(key, count);
        				IntField countAggregatorResult = new IntField(count);
        				// 在list里面查找gbfield对应的tuple
        				Iterator<Tuple> it = this.list.iterator();
        				while (it.hasNext())
        				{
        					tempTuple = it.next();
        					if (((IntField)(tempTuple.getField(0))).equals(tempGbField))
        					{
        						tempTuple.setField(1, countAggregatorResult);
        						break;
        					}
        				}
        			}
        			else
        			{
        				this.countHash_Int.put(key, 1);
        				IntField countAggregatorResult = new IntField(1);
        				Type[] tempType = new Type[2];
            			tempType[0] = Type.INT_TYPE;
                		tempType[1] = Type.INT_TYPE;
                		TupleDesc tempTupleDesc = new TupleDesc(tempType);
            			tempTuple = new Tuple(tempTupleDesc);
            			tempTuple.setField(0, tempGbField);
            			tempTuple.setField(1, countAggregatorResult);
            			// 再加入到merge结果的新表中
            			this.list.add(tempTuple);
        			}
    			}
    			else
    			{
    				StringField tempGbField = (StringField)tempField;
        			String key = tempGbField.getValue();
        			Tuple tempTuple = null;
        			if (this.countHash.containsKey(key))
        			{
        				int count = this.countHash.get(key) + 1;
        				this.countHash.put(key, count);
        				IntField countAggregatorResult = new IntField(count);
        				// 在list里面查找gbfield对应的tuple
        				Iterator<Tuple> it = this.list.iterator();
        				while (it.hasNext())
        				{
        					tempTuple = it.next();
        					if (((StringField)(tempTuple.getField(0))).equals(tempGbField))
        					{
        						break;
        					}
        				}
        				if (!it.hasNext())
        				{
        					System.out.println("Do not find gbfield in list, but gbfield contained in HashMap!");
        				}
            			tempTuple.setField(1, countAggregatorResult);
        			}
        			else
        			{
        				IntField countAggregatorResult = new IntField(1);
        				Type[] tempType = new Type[2];
            			tempType[0] = Type.STRING_TYPE;
                		tempType[1] = Type.INT_TYPE;
                		TupleDesc tempTupleDesc = new TupleDesc(tempType);
            			tempTuple = new Tuple(tempTupleDesc);
            			tempTuple.setField(0, tempGbField);
            			tempTuple.setField(1, countAggregatorResult);
            			// 再加入到merge结果的新表中
            			this.list.add(tempTuple);
        			}
    			}
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	return this.stringAggregatorIterator;
    }

}
