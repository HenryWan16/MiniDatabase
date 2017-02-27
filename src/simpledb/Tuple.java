package simpledb;

import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {
	private TupleDesc td = null;
	// What is the recordId? 页号加页内偏移
	private RecordId rid = null;
	private Field[] field = null;
	private int size = 0;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	this.td = td;
    	this.size = td.numFields();
    	this.field = new Field[size];
    	for (int i = 0; i < this.td.numFields(); i++)
    	{
    		field[i] = null;
    	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if (i >= this.td.numFields() || i < 0)
    	{
    		throw new NoSuchElementException();
    	}
    	else
    	{
    		this.field[i] = f;
    	}
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	if (i >= this.td.numFields() || i < 0)
    	{
    		throw new NoSuchElementException();
    	}
    	return this.field[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
//        throw new UnsupportedOperationException("Implement this");
    	String temp = "";
    	for (int i = 0; i < this.size; i++)
    	{
    		temp += field[i];
    		if (i != this.size - 1)
    		{
    			temp += "\t";
    		}
    	}
    	temp += "\n";
    	return temp;
    }
}
