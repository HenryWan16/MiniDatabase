package simpledb;

/** Predicate compares tuples to a specified Field value.
 */
public class Predicate {
	private Field operand = null;
	private Op op = null;
	private int fieldNumber = 0;
	
    /** Constants used for return codes in Field.compare */
    public enum Op {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
    }

    /**
     * Constructor.
     *
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
    	this.fieldNumber = field;
    	this.op = op;
    	this.operand = operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific
     * in the constructor.  The comparison can be made through Field's
     * compare method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    // t(fieldNum) op operand ? true : false 位置完全对应
    public boolean filter(Tuple t) {
        // some code goes here
    	// this.operand this.op 传入tuple的第几个field 
    	// 例如this.operand 是 > 号，则判断传入的t的field参数是不是比初始化到对象里的值大
    	// 是则返回true，即t对应下标对应的值大于this.operand则返回true
    	Field temp = t.getField(this.fieldNumber);
    	boolean result = false;
    	result = temp.compare(this.op, this.operand);
    	return result;
    }

    /**
     * Returns something useful, like
     * "f = field_id op = op_string operand = operand_string
     */
    public String toString() {
        // some code goes here
    	String str = "";
    	str = "f = " + this.fieldNumber + " op = " + this.op.toString() + " operand = " + this.operand.toString();
        return str;
    }
}






