package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
	private Op op = null;
	private int gbfield = 0;
	private int afield = 0;
	private Type gbfieldtype = null;
	private TupleDesc tupleDesc = null;	// 用来描述IntAggregator的表Scheme
	private ArrayList<Tuple> list = new ArrayList<Tuple>();
	private IntAggregatorIterator intAggregatorIterator = null;
	// 聚集函数的结果值，仅仅用于No_group情况
	private int count = 0;
	private int sum = 0;
	private int max = 0;
	private int min = 0;
	private int avg = 0;
	// 用于存放所有的要求平均值的整型数
	private ArrayList<Field> avgList_Nogroup = new ArrayList<Field>();
	
	// 用于group_by时候记录每个index的对应Field信息，用来求avg的值；
	private HashMap<Integer, ArrayList<Field>> avgCount_hash = new HashMap<Integer, ArrayList<Field>>();
	
	private class IntAggregatorIterator implements DbIterator
	{
		Iterator<Tuple> it = null;
		
		public IntAggregatorIterator()
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
		
		public IntAggregatorIterator(ArrayList<Tuple> l)
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
			// TODO Auto-generated method stub
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
				throw new NoSuchElementException("tuple is null (IntAggregator)");
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
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.afield = afield;
    	this.gbfieldtype = gbfieldtype;
    	this.op = what;
    	
    	// 遇到No_Grouping，初始化为(aggregateValue)，而此时类型仅仅为int，即Type.INT_TYPE
    	if (gbfieldtype == null)
    	{
    		Type[] tempType = new Type[1];
    		tempType[0] = Type.INT_TYPE;
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
    		tempType[1] = Type.INT_TYPE;
    		this.tupleDesc = new TupleDesc(tempType);
    	}
    	this.intAggregatorIterator = new IntAggregatorIterator(this.list);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    // 构造出与Op对应的ArrayList<Tuple>，（依据某一属性分的组，op对某一属性处理结果)或者(op对某一属性处理结果)
    // 两种TupleDesc有本质的不同，仅有一个元素的list可以直接在this.count上累加，而键值对的情况不行，一定要先取得值后修改
    // 再次写回
    public void merge(Tuple tup) {
        // some code goes here
    	int length = this.list.size();
    	Tuple temp = new Tuple(this.tupleDesc);
    	int index = 0;
    	Field aggregatorResult = null;
    	Field tempField = null;
    	
    	// 刚开始的时候list为空，且可能有两种TupleDesc，但在执行构造方法以后就确定了是那种TupleDesc了，op是否为count
    	// 初始状态list为空
    	if (length == 0)
    	{
    		this.count = 1;
    		aggregatorResult = tup.getField(this.afield);
    		this.sum = ((IntField)aggregatorResult).getValue();
    		this.min = ((IntField)aggregatorResult).getValue();
    		this.max = ((IntField)aggregatorResult).getValue();
    		this.avg = ((IntField)aggregatorResult).getValue();
    		// No_GroupBy
	    	if (this.tupleDesc.numFields() == 1)
	    	{
	    		if (this.op == Op.COUNT)
	    		{
	    			temp.setField(0, new IntField(this.count));
	    		}
	    		else
	    		{
	    			if (this.op == Op.AVG)
	    			{
	    				this.avgList_Nogroup.add(tup.getField(this.afield));
	    			}
	    			temp.setField(0, aggregatorResult);
	    		}
	    	}
	    	else
	    	{
	    		if (this.op == Op.COUNT)
	    		{
	    			temp.setField(0, tup.getField(this.gbfield));
	    			temp.setField(1, new IntField(this.count));
	    		}
	    		else
	    		{
	    			if (this.op == Op.AVG)
	    			{
	    				ArrayList<Field> tempAvgArrayList = new ArrayList<Field>();
	    				tempAvgArrayList.add(aggregatorResult);
	    				this.avgCount_hash.put(0,tempAvgArrayList);
	    			}
	    			temp.setField(0, tup.getField(this.gbfield));
	    			temp.setField(1, aggregatorResult);
	    		}
	    	}
	    	this.list.add(temp);
    	}
    	// list里面已经有数据了，则先查询组信息后更改聚集函数值
    	else
    	{
    		// No_group 则结果中仅仅有一个聚集函数的值，无须遍历整个表
    		if (this.tupleDesc.numFields() == 1)
    		{
    			switch (this.op)
    			{
    			case COUNT: {
			    				tempField = tup.getField(this.afield);
								if (tempField != null)
								{
									this.count++;
				    				temp.setField(0, new IntField(this.count));
								}
								break;
    						}
    			case MAX:	{
			    				tempField = tup.getField(this.afield);
			    				if (tempField != null)
								{
									IntField maxField1 = (IntField)list.get(0).getField(0);
									this.max = maxField1.getValue();
									IntField maxField2 = (IntField)tempField;
									if (this.max < maxField2.getValue())
									{
										this.max = maxField2.getValue();
										aggregatorResult = tup.getField(this.afield);
										temp.setField(0, aggregatorResult);
										this.list.set(0, temp);
									}
								}
								break;
							}
    			case MIN:	{
			    				tempField = tup.getField(this.afield);
								if (tempField != null)
								{
									IntField minField1 = (IntField)list.get(0).getField(0);
									this.min = minField1.getValue();
									IntField minField2 = (IntField)tempField;
									if (this.min > minField2.getValue())
									{
										this.min = minField2.getValue();
										aggregatorResult = tup.getField(this.afield);
										temp.setField(0, aggregatorResult);
										this.list.set(0, temp);
									}
								}
								break;
							}
    			case SUM: 	{
			    				tempField = tup.getField(this.afield);
								if (tempField != null)
								{
									IntField tempIntField = (IntField)tempField;
									IntField listIntField = (IntField)list.get(0).getField(0);
									this.sum = listIntField.getValue() + tempIntField.getValue();
									aggregatorResult = new IntField(this.sum);
									temp.setField(0, aggregatorResult);
				    				this.list.set(0, temp);
								}
								break;
							}
    			case AVG:	{
			    				tempField = tup.getField(this.afield);
			    				this.avgList_Nogroup.add(tempField);
			    				int avg_count = this.avgList_Nogroup.size();
			    				int sum = 0;
								if (tempField != null)
								{
									for (Iterator<Field> it = this.avgList_Nogroup.iterator(); it.hasNext();)
									{
										sum = sum + ((IntField)it.next()).getValue();
									}
									this.avg = sum/avg_count;
									aggregatorResult = new IntField(this.avg);
									temp.setField(0, aggregatorResult);
				    				this.list.set(0, temp);
								}
								break;
							}
    			}
    		}
    		else
    		{
    			index = this.findGroup(tup);
    			if (index < length)
    			{
    				this.changeAggregateValue(index, tup);
    			}
    			// 发现了新的group
    			else
    			{
    				this.insertNewGroup(tup);
    			}
    		}
    	}
    }

    /**
     * 返回一条记录所在的组的标号
     * 必须是针对有Group的情况
     * @param tuple 
     * 			将要用于合并的tuple
     * @return
     * 			需要插入或修改的位置，这里是组号，没找到则返回length
     */
    public int findGroup(Tuple tuple)
    {
    	int index = 0 , i = 0;
    	int length = list.size();
    	Field listField = null;
    	Field tupleField = tuple.getField(this.gbfield);
    	if (length == 0)
    	{
    		return index;
    	}
    	for (i = 0; i < length; i++)
    	{
    		listField = list.get(i).getField(0);
    		if (listField.compare(Predicate.Op.EQUALS, tupleField))
    		{
    			index = i;
    			break;
    		}
    	}
    	index = i;
    	return index;
    }
    
    /**
     * 求得新的聚集函数值并且修改list中的聚集函数的值
     * 不同种类的聚集函数得到不同的值
     * 此时需要取出list里面的聚集函数值，在这个值上做变化后写入原来的list位置中
     * 而不能直接使用this.count
     * @param index
     * 			需要修改聚集函数值的组号
     * 			TupleDesc的类型必须是有group by
     * @param tup
     * 			需要更改的值
     */
    public void changeAggregateValue(int index, Tuple tup)
    {
    	Field aggregatorResult = null;
    	Tuple temp = new Tuple(this.tupleDesc);
    	Field tempAField = null;
    	Field tempGbField = null;
    	ArrayList<Field> tempAvgArrayList = null;
    	int count = 0;
    	int sum = 0;
    	int min = 0;
    	int max = 0;
    	int avg = 0;
    	switch (this.op)
		{
		case COUNT: {
	    				tempAField = tup.getField(this.afield);
	    				count = ((IntField)this.list.get(index).getField(1)).getValue();
						if (tempAField != null)
						{
							count++;
							aggregatorResult = new IntField(count);
							temp.setField(0, tup.getField(this.gbfield));
							temp.setField(1, aggregatorResult);
							this.list.set(index, temp);
						}
						break;
					}
		case MAX:	{
						tempAField = tup.getField(this.afield);
						max = ((IntField)this.list.get(index).getField(1)).getValue();
						if (tempAField != null)
						{
							IntField maxField2 = (IntField)tempAField;
							if (max < maxField2.getValue())
							{
								aggregatorResult = tempAField;
								temp.setField(0, tup.getField(this.gbfield));
								temp.setField(1, aggregatorResult);
								this.list.set(index, temp);
							}
						}
						break;
					}
		case MIN:	{
						tempAField = tup.getField(this.afield);
						min = ((IntField)this.list.get(index).getField(1)).getValue();
						if (tempAField != null)
						{
							IntField minField2 = (IntField)tempAField;
							if (min > minField2.getValue())
							{
								aggregatorResult = tempAField;
								temp.setField(0, tup.getField(this.gbfield));
								temp.setField(1, aggregatorResult);
								this.list.set(index, temp);
							}
						}
						break;
					}
		case SUM: 	{
	    				tempAField = tup.getField(this.afield);
	    				sum = ((IntField)this.list.get(index).getField(1)).getValue();
						if (tempAField != null)
						{
							IntField tempIntField = (IntField)tempAField;
							sum = sum + tempIntField.getValue();
							aggregatorResult = new IntField(sum);
							temp.setField(0, tup.getField(this.gbfield));
							temp.setField(1, aggregatorResult);
							this.list.set(index, temp);
						}
						break;
					}
		case AVG:	{
	    				tempAField = tup.getField(this.afield);
	    				tempGbField = tup.getField(this.gbfield);
						if (tempAField != null)
						{
							tempAvgArrayList = this.avgCount_hash.get(index);
							tempAvgArrayList.add(tempAField);
							this.avgCount_hash.put(index, tempAvgArrayList);
							Iterator<Field> it = this.avgCount_hash.get(index).iterator();
							while (it.hasNext())
							{
								sum = sum + ((IntField)it.next()).getValue();
							}
							count = this.avgCount_hash.get(index).size();
							avg = sum/count;
							aggregatorResult = new IntField(avg);
							temp.setField(0, tup.getField(this.gbfield));
							temp.setField(1, aggregatorResult);
							this.list.set(index, temp);
						}
						break;
					}
		default: System.out.println("Error Aggregate operator"); break;
		}
    }
    
    /**
     * 插入新的组和对应新的组的聚集函数值
     * 不同种类的聚集函数得到不同的值
     * 
     * @param tup
     * 			新插入的tup，包含很多个域，其中有标号为gbfield afield的域
     *  		已经获得的新的组号
     * 			TupleDesc的类型必须是有group by
     */
    public void insertNewGroup(Tuple tup)
    {
    	Field aggregatorResult = null;
    	Tuple temp = new Tuple(this.tupleDesc);
    	int count = 1;
    	if (this.op == Op.COUNT)
		{
			temp.setField(0, tup.getField(this.gbfield));
			temp.setField(1, new IntField(count));
		}
		else
		{
			if (this.op == Op.AVG)
			{
				// 先修改对应gbfield的Hash表HashMap<Index, ArrayList<Field>>, i 表示的是对应index的值
				Integer i = this.avgCount_hash.size();
				ArrayList<Field> tempAvgArrayList = new ArrayList<Field>();
				tempAvgArrayList.add(tup.getField(this.afield));
				this.avgCount_hash.put(i, tempAvgArrayList);
			}
			aggregatorResult = tup.getField(this.afield);
			temp.setField(0, tup.getField(this.gbfield));
			temp.setField(1, aggregatorResult);
		}
    	this.list.add(temp);
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
//        throw new UnsupportedOperationException("implement me");
    	return this.intAggregatorIterator;
    }

}
