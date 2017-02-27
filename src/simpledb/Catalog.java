package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. For now, this is a stub catalog that must be populated
 * with tables by a user program before it can be used -- eventually, this
 * should be converted to a catalog that reads a catalog table from disk.
 */

public class Catalog
{
	private Map<Integer, TableItem> cataMap;

	public static class TableItem
	{
		private DbFile dbfile;
		private String name;
		private String pkeyname;

		public TableItem(DbFile file, String name, String pkeyname)
		{
			this.dbfile = file;
			this.name = name;
			this.pkeyname = pkeyname;
		}

		public DbFile getFile()
		{
			return this.dbfile;
		}

		public String getNmae()
		{
			return this.name;
		}

		public String getPkeyName()
		{
			return this.pkeyname;
		}
	}

	/**
	 * Constructor. Creates a new, empty catalog.
	 */
	public Catalog()
	{
		this.cataMap = new HashMap<Integer, TableItem>();
	}

	/**
	 * Add a new table to the catalog. This table's contents are stored in the
	 * specified DbFile.
	 * 
	 * @param file
	 *            the contents of the table to add; file.getId() is the
	 *            identfier of this file/tupledesc param for the calls
	 *            getTupleDesc and getFile
	 * @param name
	 *            the name of the table -- may be an empty string. May not be
	 *            null. If a name
	 * @param pkeyField
	 *            the name of the primary key field conflict exists, use the
	 *            last table to be added as the table for a given name.
	 */
	public void addTable(DbFile file, String name, String pkeyField)
	{
		Integer tId = file.getId();
		if (name.equals(null))
		{
			throw new IllegalArgumentException("name cannot be null");
		}
		else
		{
			if (cataMap.containsKey(tId))
				cataMap.remove(tId);

			TableItem t = new TableItem(file, name, pkeyField);
			cataMap.put(tId, t);
		}
	}

	public void addTable(DbFile file, String name)
	{
		addTable(file, name, "");
	}

	/**
	 * Add a new table to the catalog. This table has tuples formatted using the
	 * specified TupleDesc and its contents are stored in the specified DbFile.
	 * 
	 * @param file
	 *            the contents of the table to add; file.getId() is the
	 *            identfier of this file/tupledesc param for the calls
	 *            getTupleDesc and getFile
	 * @param t
	 *            the format of tuples that are being added
	 */
	/*
	 * public void addTable(DbFile file) { addTable(file, (new
	 * UUID()).toString()); }
	 */

	/**
	 * Return the id of the table with a specified name,
	 * 
	 * @throws NoSuchElementException
	 *             if the table doesn't exist
	 */
	public int getTableId(String name) throws NoSuchElementException
	{
		//if (name == null)
			//throw new IllegalArgumentException("name must not null");

		for (TableItem t : this.cataMap.values())
		{
			if (!t.name.equals(name))
				continue;
			return t.dbfile.getId();
		}
		throw new NoSuchElementException();
	}

	/**
	 * Returns the tuple descriptor (schema) of the specified table
	 * 
	 * @param tableid
	 *            The id of the table, as specified by the DbFile.getId()
	 *            function passed to addTable
	 */
	public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException
	{
		if (this.cataMap.containsKey(tableid))
			return this.cataMap.get(tableid).getFile().getTupleDesc();
		else
			throw new NoSuchElementException();
	}

	/**
	 * Returns the DbFile that can be used to read the contents of the specified
	 * table.
	 * 
	 * @param tableid
	 *            The id of the table, as specified by the DbFile.getId()
	 *            function passed to addTable
	 */
	public DbFile getDbFile(int tableid) throws NoSuchElementException
	{
		if (this.cataMap.containsKey(tableid))
			return this.cataMap.get(tableid).getFile();
		else
			throw new NoSuchElementException();
	}

	/** Delete all tables from the catalog */
	public void clear()
	{
		this.cataMap.clear();
	}

	public String getPrimaryKey(int tableid) throws NoSuchElementException
	{
		if (this.cataMap.containsKey(tableid))
			return this.cataMap.get(tableid).getPkeyName();
		else
			throw new NoSuchElementException();
	}

	public Iterator<Integer> tableIdIterator()
	{
		// some code goes here
		return this.cataMap.keySet().iterator();
	}

	public String getTableName(int tableid) throws NoSuchElementException
	{
		if (this.cataMap.containsKey(tableid))
			return this.cataMap.get(tableid).getNmae();
		else
			throw new NoSuchElementException();
	}

	/**
	 * Reads the schema from a file and creates the appropriate tables in the
	 * database.
	 * 
	 * @param catalogFile
	 */
	public void loadSchema(String catalogFile)
	{
		String line = "";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(new File(
					catalogFile)));

			while ((line = br.readLine()) != null)
			{
				// assume line is of the format name (field type, field type,
				// ...)
				String name = line.substring(0, line.indexOf("(")).trim();
				// System.out.println("TABLE NAME: " + name);
				String fields = line.substring(line.indexOf("(") + 1,
						line.indexOf(")")).trim();
				String[] els = fields.split(",");
				ArrayList<String> names = new ArrayList<String>();
				ArrayList<Type> types = new ArrayList<Type>();
				String primaryKey = "";
				for (String e : els)
				{
					String[] els2 = e.trim().split(" ");
					names.add(els2[0].trim());
					if (els2[1].trim().toLowerCase().equals("int"))
						types.add(Type.INT_TYPE);
					else if (els2[1].trim().toLowerCase().equals("string"))
						types.add(Type.STRING_TYPE);
					else
					{
						System.out.println("Unknown type " + els2[1]);
						System.exit(0);
					}
					if (els2.length == 3)
					{
						if (els2[2].trim().equals("pk"))
							primaryKey = els2[0].trim();
						else
						{
							System.out.println("Unknown annotation " + els2[2]);
							System.exit(0);
						}
					}
				}
				Type[] typeAr = types.toArray(new Type[0]);
				String[] namesAr = names.toArray(new String[0]);
				TupleDesc t = new TupleDesc(typeAr, namesAr);
				HeapFile tabHf = new HeapFile(new File(name + ".dat"), t);
				addTable(tabHf, name, primaryKey);
				System.out.println("Added table : " + name + " with schema "
						+ t);
			}
		} catch (IOException e)
		{
			e.printStackTrace();
			System.exit(0);
		} catch (IndexOutOfBoundsException e)
		{
			System.out.println("Invalid catalog entry : " + line);
			System.exit(0);
		}
	}
	
	public List<TableItem> getTableItem()
	{
		List<TableItem> tlist = new ArrayList<TableItem>();
		for (TableItem ti : this.cataMap.values())
			tlist.add(ti);
		return tlist;
	}
}
