package simpledb;

import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */

public class TupleDesc {

	private Type[] type = null;
	private String[] name = null;
	private int size = 0;
	
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	 
    	int newSize = td1.size + td2.size;
    	Type[] tempTypeArray = new Type[newSize];
    	String[] tempStringArray = new String[newSize];
    	
    	// copy td1.type to tempTypeArray[]
    	for (int i = 0; i < td1.size; i++)
    	{
    		tempTypeArray[i] = td1.type[i];
    	}
    	for (int i = td1.size; i < newSize; i++)
    	{
    		tempTypeArray[i] = td2.type[i - td1.size];
    	}
    	
    	// copy td1.name to tempStringArray[], but how to deal with anonymous fields   	
    	for (int i = 0; i < td1.size; i++)
    	{
    		tempStringArray[i] = td1.name[i];
    	}
    	for (int i = td1.size; i < newSize; i++)
    	{
    		tempStringArray[i] = td2.name[i - td1.size];
    	}
    	
    	TupleDesc temp = new TupleDesc(tempTypeArray, tempStringArray);
    	
        return temp;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	this.type = typeAr;
    	this.name = fieldAr;
    	this.size = typeAr.length;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.type = typeAr;
    	this.size = typeAr.length;
    	this.name = new String[this.size];
    	for (int i = 0; i < this.size; i++)
    	{
    		this.name[i] = null;
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.size;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i >= this.size || i < 0)
    	{
    		throw new NoSuchElementException();
    	}
    	else
    	{
    		return this.name[i];
    	}
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String namex) throws NoSuchElementException {
        // some code goes here
    	int index = 0;
    	int i = 0;
    	if (namex == null)
    	{
    		throw new NoSuchElementException();
    	}
    	while (i < this.size)
    	{
    		if (namex.equals(this.name[i]))
    		{
    			index = i;
    			break;
    		}
    		i++;
    	}
    	if (i == this.size)
    	{
    		// FIXME debug info
//    		System.out.println("All the TypeName in this table is:");
//    		for (int j = 0; j < this.size; j++)
//    		{
//    			System.out.print(this.name[j] + " ");
//    		}
//    		System.out.println("");
//    		System.out.println(namex + " is not in the table!");
    		throw new NoSuchElementException();
    	}
        return index;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i >= this.size || i < 0)
    	{
    		throw new NoSuchElementException();
    	}
    	else
    	{
    		return this.type[i];
    	}
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int tempSize = 0;
    	for (Type t : this.type)
    	{
    		tempSize += t.getLen();
    	}
        return tempSize;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	TupleDesc temp = null;
    	if (!(o instanceof TupleDesc))
    	{
    		return false;
    	}
    	else
    	{
    		temp = (TupleDesc)o;
    	}
    	if (temp.size != this.size)
    	{
    		return false;
    	}
    	else
    	{
    		for (int i = 0; i < this.size; i++)
    		{
    			if (!temp.type[i].equals(this.type[i]))
    			{
    				return false;
    			}
    		}
    		return true;
    	}
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String temp = "";
    	for (int i = 0; i < this.size; i++)
    	{
    		if (this.type[i] == Type.INT_TYPE)
    		{
    			temp += "INT";
    		}
    		else
    		{
    			temp += "STRING";
    		}
    		temp += "(" + this.name[i] + ")";
    		if (i != this.size - 1)
    		{
    			temp += ",";
    		}
    	}
        return temp;
    }
}
