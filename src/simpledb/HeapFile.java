package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile
{

	File f;
	TupleDesc td;
	int tableId;
	
	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td)
	{
		// some code goes here
		this.f = f;
		this.td = td;
		this.tableId = this.getId();
	}

	public static class HeapFileIterator implements DbFileIterator
	{

		TransactionId tid;
		Iterator<Tuple> it;
		
		// 使用HeapPage里的迭代器，访问HeapPage里的每条有效的Tuple
		// 把访问到的有效Tuple放到一个List里面
		// 返回list.iterator();
		List<Tuple> list;

		public HeapFileIterator(TransactionId tid, List<Tuple> list)
		{
			this.tid = tid;
			this.list = list;
//			try {
//				this.open();
//			} catch (DbException | TransactionAbortedException e) {
//				e.printStackTrace();
//			}
		}

		// 迭代器的open方法，实质是初始化了一个迭代器
		@Override
		public void open() throws DbException, TransactionAbortedException
		{
			it = this.list.iterator();
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException
		{
			if (it == null)
				return false;
			
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException
		{
			if (it == null)
				throw new NoSuchElementException("tuple is null");
			
			return it.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException
		{
			this.close();
			this.open();
		}

		@Override
		public void close()
		{
			this.it = null;
		}

	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile()
	{
		// some code goes here
		return this.f;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId()
	{
		return this.f.getAbsoluteFile().hashCode();
		// throw new UnsupportedOperationException("implement this");
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return this.td;
		// throw new UnsupportedOperationException("implement this");
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid)
	{
		// some code goes here
		try
		{
			// 文件指针
			RandomAccessFile rAf = new RandomAccessFile(f, "r");
			// 根据page编号得到偏移量
			int offset = pid.pageno() * BufferPool.PAGE_SIZE;
			// 移动文件指针
			rAf.seek(offset);
			// 新page的内容
			byte[] page = new byte[BufferPool.PAGE_SIZE];
			// TODO 剩余文件不足BufferPool.PAGE_SIZE怎么办？
			rAf.read(page, 0, BufferPool.PAGE_SIZE);
			rAf.close();

			HeapPageId id = (HeapPageId) pid;

			return (Page) new HeapPage(id, page);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		throw new IllegalArgumentException();
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException
	{
		// some code goes here
		// not necessary for lab1
		// 要写入文件的page内容
		byte[] page_content = page.getPageData();
		PageId pid = page.getId();
		// 文件指针
		RandomAccessFile rAf = new RandomAccessFile(f, "rw");
		// 根据page编号得到偏移量
		int offset = pid.pageno() * BufferPool.PAGE_SIZE;
		if (offset < rAf.length())
		{
			// 移动文件指针
			rAf.seek(offset);
			rAf.write(page_content);
			rAf.close();
		}
		else
		{
			rAf.seek(rAf.length());
			rAf.write(page_content);
			rAf.close();
		}
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	// HeapFile里面有多少个Page需要看对应的File的大小是多少
	public int numPages()
	{
		// some code goes here
		long file_size = this.f.length();
		int num = (int) (file_size / BufferPool.PAGE_SIZE);
		if (file_size % BufferPool.PAGE_SIZE > 0)
			num++;
		return num;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException
	{
		// some code goes here
		// not necessary for lab1
		ArrayList<Page> pages_update = new ArrayList<Page>();
		HeapPage page = null;
		int page_total = this.numPages();
		int i = 0;
		for (;i < page_total; i++)
		{
			PageId pid = new HeapPageId(this.tableId, i);
			// 获得该表里的一个page，检查Header，看看有没有空的slot可以插入
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			// 没有空的slot，说明该页满，这时候检测下一个page
			if (page.getNumEmptySlots() == 0)
			{
				continue;
			}
			// 有空的slot，则找到页里的第一个空的slot，设置好tuple的recordId
			for (int j = 0; j < page.numSlots; j++)
			{
				if (page.getSlot(j))
				{
					continue;
				}
				t.setRecordId(new RecordId(pid, j));
				page.addTuple(t);
				if (Debug.isEnabled(3))
				{
					Debug.log("HeapFile addTuple: page_total = " + page_total);
					Iterator<Tuple> it = page.iterator();
					while (it.hasNext() && i == (page_total - 1))
					{
						Tuple debugTuple = it.next();
						if (Debug.isEnabled(2))
						{
							Debug.log(debugTuple.toString());
						}
					}
				}
				pages_update.add(page);
				return pages_update;
			}
		}
		// 表中所有page都是满的，则做出一个新的Page，将Tuple放入该page(包括写表头、加入Tuple信息和markDirty()，这些都交给HeapPage来做了)，然后写入对应的File文件中
		// 为了使得页数增加必须要写文件
		if (i == page_total)
		{
			HeapPageId pid = new HeapPageId(this.tableId, i);
			// 不使用BuffePool.getPage()获得新的page，此时是写文件
			byte[] empty_data = HeapPage.createEmptyPageData();
			page = new HeapPage(pid, empty_data);
			t.setRecordId(new RecordId(pid, 0));
			page.addTuple(t);
			this.writePage(page);
			if (Debug.isEnabled(3))
			{
				Debug.log("HeapFile addTuple: page_total = " + page_total);
				Iterator<Tuple> it = page.iterator();
				while (it.hasNext() && i == (page_total - 1))
				{
					Tuple debugTuple = it.next();
					if (Debug.isEnabled(2))
					{
						Debug.log(debugTuple.toString());
					}
				}
			}
			pages_update.add(page);
			return pages_update;
		}
		// 进行过insert操作以后，原有的Iterator不再能使用了，必须重新使用构造器iterator获得新的迭代器；
//		HeapFileIterator hfIterator = (HeapFileIterator) this.iterator(tid);
		return pages_update;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException
	{
		// some code goes here
		// not necessary for lab1
		PageId pageId = t.getRecordId().getPageId(); 
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);
		return page;
	}

	// see DbFile.java for javadocs
	/**
	 * 使用了缓冲区BuferPool.getPage()来提速
	 * 得到每个page的迭代器
	 * 把每个page里面的header bit 不是False的Tuple加入到HeapFileIterator的List里面
	 * 最后获得List的迭代器
	 */
	public DbFileIterator iterator(TransactionId tid) throws TransactionAbortedException,DbException
	{
		// some code goes here
		//将文件中所有page的Tuple组成一个list。
		List<Tuple> ftupleList = new ArrayList<Tuple>();
		int page_count = this.numPages();
		
		for (int i = 0; i < page_count; i++)
		{
			PageId pid = new HeapPageId(getId(), i);
			int tableid = getId();
			for ( PageId p : Database.getBufferPool().pageMap.keySet())
			{
				if (p.equals(pid))
					pid = p;
			}
			if (pid == null)
				pid = new HeapPageId(tableid, i);
			HeapPage page = null;
			// FIXME 插入页面后， getPage得到的居然不是新的页， 跟没改过一样！
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
					Permissions.READ_WRITE);
			Iterator<Tuple> it = page.iterator();
//			if (Debug.isEnabled(2))
//			{
//				Debug.log("HeapFile Iterator: page_count = " + page_count);
//				while (it.hasNext() && i == (page_count - 1))
//				{
//					Tuple debugTuple = it.next();
//					// 真正只有这句代码有用
//					ftupleList.add(debugTuple);
//					if (Debug.isEnabled(2))
//					{
//						Debug.log(debugTuple.toString());
//					}
//				}
//			}
			if (Debug.isEnabled(2))
			{
				Debug.log("HeapFile Iterator: page_count = " + (page_count - 1));
			}
			while (it.hasNext())
			{
				Tuple debugTuple = it.next();
				ftupleList.add(debugTuple);
				if (Debug.isEnabled(2) && i == (page_count - 1))
				{
					Debug.log("In HeapFile PageNo " + i + " : " + debugTuple.toString());
				}
			}
		}
		return new HeapFileIterator(tid, ftupleList);
	}
}
