package simpledb;

import java.io.File;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator
{
	TransactionId tid;
	int tableid;
	String tableAlias;
	DbFileIterator it;
	// HeapFile中public static class HeapFileIterator implements DbFileIterator

	/**
	 * Creates a sequential scan over the specified table as a part of the
	 * specified transaction.
	 *
	 * @param tid
	 *            The transaction this scan is running as a part of.
	 * @param tableid
	 *            the table to scan.
	 * @param tableAlias
	 *            the alias of this table (needed by the parser); the returned
	 *            tupleDesc should have fields with name tableAlias.fieldName
	 *            (note: this class is not responsible for handling a case where
	 *            tableAlias or fieldName are null. It shouldn't crash if they
	 *            are, but the resulting name can be null.fieldName,
	 *            tableAlias.null, or null.null).
	 *            这个是表的别名，在Parser中的用法如下：
	 *            SimpleDB> select d.f1, d.f2 from data d; d 就是tableAlias
	 *            后面的TupleDesc里面的方法nameToId（String name）实际上用的是d.fieldname去做field的判断
	 * @throws DbException 
	 * @throws TransactionAbortedException 
	 * @throws NoSuchElementException 
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias) throws NoSuchElementException, TransactionAbortedException, DbException
	{
		// some code goes here
		this.tid = tid;
		this.tableid = tableid;
		this.tableAlias = tableAlias;
		this.it = Database.getCatalog().getDbFile(tableid).iterator(tid);
		// FIXME 把下面的语句注释掉了，改成了这行 in lab3
		if (this.it instanceof HeapFile.HeapFileIterator)
		{
			((HeapFile.HeapFileIterator)(this.it)).it = ((HeapFile.HeapFileIterator)(this.it)).list.iterator();
		}
//		try {
//			it.open();
//			if (Debug.isEnabled(2))
//			{
//				Debug.log("SeqScan: ");
//				while (it.hasNext())
//				{
//					Debug.log(it.next().toString());
//				}
//			}
//			it.rewind();
//		} catch (DbException e) {
//			e.printStackTrace();
//		} catch (TransactionAbortedException e) {
//			e.printStackTrace();
//		}
	}

	public void open() throws DbException, TransactionAbortedException
	{
		// some code goes here
		it.open();
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile,
	 * prefixed with the tableAlias string from the constructor.
	 * 
	 * @return the TupleDesc with field names from the underlying HeapFile,
	 *         prefixed with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		DbFile f = Database.getCatalog().getDbFile(tableid);
		int field_count = f.getTupleDesc().numFields();
		TupleDesc td = f.getTupleDesc();
		Type[] type = new Type[field_count];
		String[] name = new String[field_count];
		String prefix = null;
		
		if (this.tableAlias != null)
		{
			prefix = this.tableAlias;
		}
		else
		{
			prefix = "";
		}
		
		for (int i=0; i<field_count; i++)
		{
			String fieldname = "null";
			type[i] = td.getType(i);
			if (td.getFieldName(i) != null)
				fieldname = td.getFieldName(i);
			// XXX Having been fixed by Lab2.
			name[i] = prefix + "." + fieldname;
		}
		
		return new TupleDesc(type, name);
	}

	public boolean hasNext() throws TransactionAbortedException, DbException
	{
		// some code goes here
		return it.hasNext();
	}

	public Tuple next() throws NoSuchElementException,
			TransactionAbortedException, DbException
	{
		// some code goes here
		return it.next();
	}

	public void close()
	{
		// some code goes here
		it.close();
	}

	public void rewind() throws DbException, NoSuchElementException,
			TransactionAbortedException
	{
		// some code goes here
		it.rewind();
	}
}
