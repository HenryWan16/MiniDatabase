package simpledb;

import java.util.ArrayList;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int buckets;
	private int min;
	private int max;
	private int ntups;
	
	// 每个桶的平均长度，比如v的值在0-31之间，共有100个buckets，则直接32/100
	private double avg;
	// 每个桶的宽度， 即表示记录的范围(int 肯定不行，比如min = 5, max = 48, buckets = 10, w会在4 5中间)，可能为0
	private long bucketWidth[];
	// 区间内统计值的个数
	private long[] h_array;
	// 每个区间的端点值
	private long[] b;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
    	this.min = min;
    	// 处理最后一个数会单独成立出一组，造成数组溢出的情况 故规定如下：
    	// 第一个长条左右边界都属于第一个长条，之后的左边界属于前一个长条，右边界属于自己，在getIndex方法里面实现
    	// 或者this.max = max + 1； 一般情况下this.max是娶不到的
    	// 但是要解决比如min = 1, max = 10, 要分到10个桶子里面各个w的值中最后一个是0，所以一直向后面扫描到w不为0的
    	// 开始加一，但是后果是b[] = {1, 1, 2}的时候，求等于2时候的选择代价的时h/w(实际上算的是1的代价), 小于2时候的代价出现了重复计算
    	this.max = max;
    	this.bucketWidth = new long[buckets];
    	this.avg = (this.max - this.min + 0.0)/this.buckets;
    	this.h_array = new long[this.buckets];
    	this.b = new long[this.buckets + 1];
    	this.ntups = 0;
    	for (int i = 0; i < this.buckets; i++)
    	{
    		h_array[i] = 0;
    	}
    	this.b[0] = min;
    	this.bucketWidth[0] = 1;
    	
    	// bucket的边界只能是整数，所以统计的是左边界
    	for (int i = 1; i < this.buckets; i++)
    	{
    		double width_temp = this.avg * i;
    		this.b[i] = (int)(this.min + width_temp);
    	}
    	
    	// 根据每个桶的左右边界求出桶的宽度width
    	// 一定要保证所有整型数所在的bucket的width为1
    	for (int i = 1; i < this.buckets; i++)
    	{
    		if (this.b[i] == this.b[i - 1])
    		{
    			this.bucketWidth[i] = 0;
    		}
    		else
    		{
    			this.bucketWidth[i] = this.b[i] - this.b[i - 1];
    		}
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int index = this.getIndex(v);
    	// 遇到min则算在h_array[0]里面，EQUAL操作中+1，LESSTHAN操作为0
    	if (index == -1)
    	{
    		System.out.println("Error,v is less than min");
    		return;
    	}
    	else if (index == -2)
    	{
    		System.out.println("Error,v is more than max");
    		return;
    	}
    	else
    	{
    		this.h_array[index]++;
    	}
    	this.ntups++;
    	System.out.println("Adding " + v + ": Counting " + this.ntups);
    }
    
    /**
     * 根据给出的整数值的大小判断其位置
     * min <= v < max 形式
     * v小于左边界返回-1
     * v大于右边界返回-2
     * v其他返回是第几个桶位， return 0 表示在第0个桶
     * @param v
     * @return
     */
    private int getIndex(int v)
    {
    	int index = 0;
    	int i = 0;
    	// v < min;
    	if (v < this.b[0])
		{
			return -1;
		}
    	// 大于等于右边界
		else if (v >= this.max)
		{
			return -2;
		}
		else
		{
//	    	for (i = 0; i < this.buckets; i++)
//	    	{
//	    		if (v > this.b[i] && v <= this.b[i + 1])
//	    		{
//	    			index = i;
//	    			break;
//	    		}
//	    		else
//	    		{
//	    			;
//	    		}
//	    	}
			// 求出相应v的下标
			index = Math.min((int)((v - this.min)/this.avg), this.buckets - 1);
		}
    	return index;
    }
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here 
    	// GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ
    	double answer = 0.0;
    	double sum = 0.0;
    	int index = this.getIndex(v);
    	if (Debug.isEnabled(3))
    	{
    		Debug.log("estimateSelectivity v is " + v);
    	}
    	if (Debug.isEnabled(3))
    	{
    		Debug.log("estimateSelectivity index is " + index);
    	}
    	// v等于一个大于右边界的数的时候， 规定为0.0，这一点在测试程序TableStatsTest程序里面已经明确
    	if (index == -2 && (op == Predicate.Op.EQUALS))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.EQUALS: return 0.0");
        	}
    		return 0.0;
    	}
    	if (index == -2 && (op == Predicate.Op.NOT_EQUALS))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.EQUALS: return 0.0");
        	}
    		return 1.0;
    	}
    	// v值大于右边界
    	if (index == -2 && (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.GREATER_THAN GREATER_THAN_OR_EQ: return 0.0");
        	}
    		return 0.0;
    	}
    	if (index == -2 && (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.LESS_THAN LESS_THAN_OR_EQ: return 1.0");
        	}
    		return 1.0;
    	}
    	// v等于一个小于左边界的数的时候， 规定为0.0，这一点在测试程序TableStatsTest程序里面已经明确
    	if (index == -1 && (op == Predicate.Op.EQUALS))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.EQUALS: return 0.0");
        	}
    		return 0.0;
    	}
    	if (index == -1 && (op == Predicate.Op.NOT_EQUALS))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.EQUALS: return 0.0");
        	}
    		return 1.0;
    	}
    	// v值小于左边界
    	if (index == -1 && (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.GREATER_THAN GREATER_THAN_OR_EQ: return 1.0");
        	}
    		return 1.0;
    	}
    	if (index == -1 && (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ))
    	{
    		if (Debug.isEnabled(3))
        	{
        		Debug.log("estimateSelectivity index Predicate.Op.LESS_THAN LESS_THAN_OR_EQ: return 0.0");
        	}
    		return 0.0;
    	}
    	double b_left = this.b[index];
    	double b_right = this.b[index + 1];
    	switch(op)
    	{
    	// (h/w)
    	case EQUALS: 
    		{
    			if (this.bucketWidth[index] != 0)
    			{
    				answer = (h_array[index] + 0.0)/this.bucketWidth[index]; 
    			}
    			else
    			{
    				answer = 0.0;
    			}
//    			answer = 1.0/width;
    			if (Debug.isEnabled(3))
            	{
    				Debug.log("avg width is :" + this.avg);
            		Debug.log("estimateSelectivity index Predicate.Op.EQUALS: return " + answer);
            		Debug.log("h=" + h_array[index] + " w=" + this.bucketWidth[index]);
            	}
    			break;
//    			int value_attribute = 0;
//    			for (int i = 0; i < buckets; i++)
//    			{
//    				if (this.h_array[i] != 0)
//    				{
//    					value_attribute++;
//    				}
//    			}
//    			return 1/value_attribute;
    		}
    	// b_f * b_part; b_f = h_b/ntups;  b_part = (b_right-const)/w_b; 大的求和
    	case GREATER_THAN: 
    		{
    			answer = (h_array[index] + 0.0)*(b_right - v)/this.bucketWidth[index];
    			for (int i = (index + 1); i < buckets; i++)
    			{
    				sum += h_array[i] + 0.0;
    			}
    			answer += sum;
    			if (Debug.isEnabled(3))
            	{
            		Debug.log("estimateSelectivity index Predicate.Op.GREATER_THAN: return " + answer);
            		Debug.log("h=" + h_array[index] + " w=" + (this.max - v));
            	}
    			break;
    		}
    	case LESS_THAN: 
    		{
    			answer = (h_array[index] + 0.0) * (v - b_left)/this.bucketWidth[index];
    			for (int i = (index - 1); i >= 0; i--)
    			{
    				sum += (h_array[i] + 0.0);
    			}
    			answer += sum;
    			if (Debug.isEnabled(3))
            	{
            		Debug.log("estimateSelectivity index Predicate.Op.LESS_THAN: return " + answer);
            		Debug.log("h=" + h_array[index] + " w=" + (v - this.min));
            	}
    			break;
    		}
    	case GREATER_THAN_OR_EQ:
    		{
    			// EQ
    			if (this.bucketWidth[index] != 0)
    			{
    				answer = (h_array[index] + 0.0)/this.bucketWidth[index]; 
    			}
    			else
    			{
    				answer = 0.0;
    			}
    			// GREATER_THAN
    			answer += (h_array[index] + 0.0)*(b_right - v)/this.bucketWidth[index];
    			for (int i = (index + 1); i < buckets; i++)
    			{
    				sum += h_array[i] + 0.0;
    			}
    			answer += sum;
    			if (Debug.isEnabled(3))
            	{
            		Debug.log("estimateSelectivity index Predicate.Op.GREATER_THAN_OR_EQ: return " + answer);
            		Debug.log("h=" + h_array[index] + " w=" + (this.max - v));
            	}
    			break;
    		}
    	case LESS_THAN_OR_EQ:
    		{
    			// EQ
    			if (this.bucketWidth[index] != 0)
    			{
    				answer = (h_array[index] + 0.0)/this.bucketWidth[index]; 
    			}
    			else
    			{
    				answer = 0.0;
    			}
    			// LESS_THAN
    			answer += (h_array[index] + 0.0) * (v - b_left)/this.bucketWidth[index];
    			for (int i = (index - 1); i >= 0; i--)
    			{
    				sum += (h_array[i] + 0.0);
    			}
    			answer += sum;
    			if (Debug.isEnabled(3))
            	{
            		Debug.log("estimateSelectivity index Predicate.Op.LESS_THAN: return " + answer);
            		Debug.log("h=" + h_array[index] + " w=" + (v - this.min));
            	}
    			break;
    		}
    	case NOT_EQUALS:
    		{
    			double answer_equal = 0.0;
    			if (this.bucketWidth[index] != 0)
    			{
    				answer_equal = (h_array[index] + 0.0)/this.bucketWidth[index]; 
    			}
    			else
    			{
    				answer_equal = 0.0;
    			}
    			answer = this.ntups - answer_equal;
    			if (Debug.isEnabled(3))
            	{
            		Debug.log("estimateSelectivity index Predicate.Op.NOT_EQUALS: return " + answer);
            		Debug.log("h=" + h_array[index] + " w=" + this.bucketWidth[index]);
            	}
    			break;
//    			return (this.max - this.min)/(this.max - this.min + 1);
    		}
    	default:
    		{
    			System.out.println("Predicate.Op op is not EQUALS GREATER_THAN, LESS_THAN.");
    			break;
    		}
    	}
    	answer = answer/this.ntups;
    	if (Debug.isEnabled(3))
    	{
    		Debug.log("this.ntups=" + this.ntups);
    		Debug.log("answer=" + answer);
    	}
    	System.out.println("EstimateSelectivity answer/this.ntups is " + answer);
    	return answer;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	String temp = "";
    	temp += "Max=" + this.max + " ; ";
    	temp += "Min=" + this.min + " ; ";
    	temp += "buckets=" + this.buckets + " ; " + "\n";
    	temp += "w: " + this.avg + "\n";
    	temp += "b[]: " + "\n";
    	for (int i = 0; i <= this.buckets; i++)
    	{
    		temp += this.b[i] + " "; 
    	}
    	temp += "\n";
    	temp += "h_array[]: " + "\n";
    	for (int i = 0; i < this.buckets; i++)
    	{
    		temp += this.h_array[i] + " "; 
    	}
        return temp;
    }
}
