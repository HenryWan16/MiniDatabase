package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    private int tableid;
    private int ioCostPerPage;
    private TupleDesc tupledesc;
    private DbFile table_Db;
    private DbFileIterator iterator;
    private HashMap<Integer, IntHistogram> intHist;
    private HashMap<Integer, StringHistogram> strHist;
    
    // 某一列的最大最小值
    private int maxInOneCulumn = -32767;
    private int minInOneCulumn = 0;
    // 某张表的总的记录条数
    private int totalNumberTuples = 0;
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	this.intHist = new HashMap<Integer, IntHistogram>();
    	this.strHist = new HashMap<Integer, StringHistogram>();
    	int buckets = NUM_HIST_BINS;
    	int int_field = 0;
    	int str_field = 0;
    	int num_field = 0;
    	
    	this.table_Db = Database.getCatalog().getDbFile(tableid);
    	this.tupledesc = table_Db.getTupleDesc();
    	num_field = this.tupledesc.numFields();
    	
    	// 貌似不能在TableStats的构造器里面另外设置tid， 也不知道会不会和已经有的tid重复
    	TransactionId tid = new TransactionId();
    	try {
			this.iterator = ((HeapFile)table_Db).iterator(tid);
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		}
    	
    	// 求得表中的记录总数
    	this.getTotalNumberTuples();
    	
    	if (Debug.isEnabled(3))
    	{
    		Debug.log("totalNumberTuples (in TableStats) is " + this.totalNumberTuples);
    	}
    	
    	for (int i = 0; i < num_field; i++)
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("New Column number (in TableStats) is " + i);
        	}
    		if (this.tupledesc.getType(i) == Type.INT_TYPE)
    		{
    			int_field++;
    			this.getMinMaxofField(i, this.iterator);
    			IntHistogram value = new IntHistogram(buckets, this.minInOneCulumn, this.maxInOneCulumn);
    			this.addValueToIntHistogram(value, i);
    			this.intHist.put(i, value);
    			// 这个最大和最小值在求完一列的属性以后要刷新
    			// 不然求第二列的属性的时候还会用第一列的属性的min和max
    			this.maxInOneCulumn = -32767;
    		    this.minInOneCulumn = 0;
    		}
    		// 针对String类型为何不要getMinMaxofField和addValueToIntHistogram呢？
    		else if (this.tupledesc.getType(i) == Type.STRING_TYPE)
    		{
    			str_field++;
    			StringHistogram value = new StringHistogram(buckets);
    			this.strHist.put(i, value);
    		}
    		else
    		{
    			System.out.println("TupleDesc Type is error!");
    			return;
    		}
    	}
    }
    
    /**
     * 求得表中某一属性的最大和最小值
     * 此时用的是遍历表中所有的Tuple一次，来寻找最大和最小值
     * 并且同时统计表中所有的记录数
     * @param num
     * 		表中从左到右数第num个属性
     * @return 无
     *  	具体的最小值， 从IntField转换来。记录在private 变量里面
     */
    private void getMinMaxofField(int num, DbFileIterator iterator)
    {
    	// 根据测试例，最小的列值为0
    	Tuple temp = new Tuple(this.tupledesc);
    	try {
	    	while (iterator.hasNext())
	    	{
				temp = iterator.next();
		    	IntField intField = (IntField) temp.getField(num);
		    	int tempInt = intField.getValue();
		    	if (tempInt < this.minInOneCulumn)
		    	{
		    		this.minInOneCulumn = tempInt;
		    	}
		    	if (tempInt > this.maxInOneCulumn)
		    	{
		    		this.maxInOneCulumn = tempInt;
		    	}
	    	}
	    	// 比如某一列从0到31的值都有，但是直方图做的是0<= v < 32的值， 所以31+1；实际32是娶不到的
	    	this.maxInOneCulumn++;
	    	iterator.rewind();
    	} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * 把指定第number列的所有元素
     * add进初始化好的直方图里
     * @param value
     * 		已经初始化好的直方图
     * @param number
     * 		要加入直方图的列号，即表的第number列
     */
    private void addValueToIntHistogram(IntHistogram value, int number)
    {
    	// 根据测试例，最小的列值为0
    	Tuple temp = new Tuple(this.tupledesc);
    	try {
	    	while (iterator.hasNext())
	    	{
				temp = iterator.next();
		    	IntField intField = (IntField) temp.getField(number);
		    	int tempInt = intField.getValue();
		    	value.addValue(tempInt);
	    	}
	    	iterator.rewind();
    	} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * 求表中共有多少条记录
     */
    private void getTotalNumberTuples()
    {
    	Tuple temp = new Tuple(this.tupledesc);
    	try {
    		// 之所以要加上rewind操作是因为之前的HeapFile中在生成一个
    		// 总的list的时候操作了内部类的it，但是仅仅next了没rewind
    		// 这是一个跨模块的耦合错误
    		this.iterator.rewind();
	    	while (this.iterator.hasNext())
	    	{
				this.totalNumberTuples++;
				this.iterator.next();
	    	}
	    	this.iterator.rewind();
    	} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * 此方法仅仅供参考，从来未被使用过
     * 返回表中某一属性的最大值
     * @param num
     * 		表中从左到右数第num个属性
     * @return result
     *  	具体的最大值， 从IntField转换来。
     */
    private int getMaxofField(int num, DbFileIterator iterator)
    {
    	int max = -32767;
    	Tuple temp = new Tuple(this.tupledesc);
    	try {
	    	while (iterator.hasNext())
	    	{
				temp = iterator.next();
		    	IntField intField = (IntField) temp.getField(num);
		    	int tempMax = intField.getValue();
		    	if (tempMax > max)
		    	{
		    		max = tempMax;
		    	}
	    	}
	    	this.iterator.rewind();
    	} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    	return max;
    }
    
    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
    	int numberPages = ((HeapFile)this.table_Db).numPages();
    	double cost_read_a_page = numberPages * this.ioCostPerPage; 
        return cost_read_a_page;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor 
     * 		The selectivity of any predicates over the table
     * 		即统计的结果值，类型为double，在0.0 - 1.0之间的一个数，代表的是直方图的统计结果
     * 		比如从中间某个数到最右边的tuple数量占了总量的百分比率为0.3
     * 		0.3 * （总共有多少个tuple）就是此时的记录数
     * @return 
     * 		The estimated cardinality of the scan with the specified selectivityFactor
     */
    // 返回指定选择代价时候的记录数
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
    	int numberTuples = 0;
    	numberTuples = (int) (selectivityFactor * this.totalNumberTuples);
//    	if (Debug.isEnabled(2))
//    	{
//    		Debug.log("selectivityFactor is " + selectivityFactor);
//    		Debug.log("totalNumberTuples is " + this.totalNumberTuples);
//    	}
        return numberTuples;
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field 
     * 		The field over which the predicate ranges
     * @param op 
     * 		The logical operation in the predicate
     * @param constant 
     * 		The value against which the field is compared
     * @return 
     * 		The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	// some code goes here
    	double result = 0.0;
    	IntHistogram intHistogram = null;
    	StringHistogram stringHistogram = null;
        if (this.intHist.containsKey(field))
        {
        	// 第几个属性域
        	System.out.println("field is " + field);
        	intHistogram = this.intHist.get(field);
        	result = intHistogram.estimateSelectivity(op, ((IntField)constant).getValue());
        }
        else if (this.strHist.containsKey(field))
        {
        	stringHistogram = this.strHist.get(field);
        	result = stringHistogram.estimateSelectivity(op, ((StringField)constant).getValue());
        }
        else
        {
        	System.out.println("field is out of range!");
        	return -1.0;
        }
    	return result;
    }
}
