package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.print.attribute.standard.PrinterInfo;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */
public class BufferPool
{
	// BufferPool能容纳的Page的大小, 一旦初始化完成，不再更改,随LRU的大小变化的是pageMap.size();
	int numPages;
	public static Map<PageId, Page> pageMap;
	public static LockManager lockManager;
	public static Map<TransactionId, Long> enduranceTimeMap;
	
	// LRU链表用来保存最近最少使用的页面PageId,规定最不经常使用的页面放在数组的末位，涉及到置换BufferPool页面，一定修改他
	ArrayList<PageId> LRU = new ArrayList<PageId>();
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;
	
	private static Lock LRULock = new ReentrantLock();
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages)
	{
		this.numPages = numPages;
		pageMap = new ConcurrentHashMap<PageId, Page>();
		lockManager = new LockManager();
		enduranceTimeMap = new ConcurrentHashMap<TransactionId, Long>();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException
	{
		// some code goes here
		// in Eviction test tid = null;
		lockManager.acquireLock(pid, tid, perm);
		if (pageMap.size() > this.numPages)
		{
			// FIXME Error
			throw (new DbException("BufferPool is full!"));
		}
		// 所请求的页面已经在BufferPool中，
		if (pageMap.containsKey(pid))
		{
			this.changeLRU(1, pid);
			Page tempPage = pageMap.get(pid);
			return tempPage;
		}
//		else
//		{
//			int fileId = pid.getTableId();
//			List<Catalog.TableItem> tlist = Database.getCatalog().getTableItem();
//			for (Catalog.TableItem t : tlist)
//			{
//				//只有当catalog里面存在对应的file时，才可以读取该页上的page。
//				if (t.getFile().getId() != fileId)
//					continue;
//				
//				DbFile file = Database.getCatalog().getDbFile(fileId);
//				Page newpage = file.readPage(pid);
//				//将新读入的page放入bufferpool
//				pageMap.put(pid, newpage);
//				
//				return newpage;
//				
//			}
//			
//			// Catalog目录里面没有这张表
//			throw new NoSuchElementException();
//			
//		}
		// implement an eviction policy which is metioned in lab1;
		else
		{
			// Catalog目录里面有没有这张表暂时不判断，默认执行该函数的时候表就是在catalog里面了
			int size = this.LRU.size();
			// BufferPool满
			if (size == this.numPages)
			{
				// 不使用No_Steal/Force策略
//				this.changeLRU(0, pid);
				// 使用No_Steal策略， 在BufferPool全部满而且都是Dirty，即再也找不到clean的页面换出的时候
				// 该异常是由于缓冲区里面的页都是dirty导致的 
				// 故把此时BufferPool所有页面flashPage到磁盘， 变成clean
				try
				{
					evictPage();
					this.changeLRU(2, pid);
				}
				catch (DbException e)
				{
					for (int i = 0; i < size; i++)
					{
						try {
							this.flushPage(this.LRU.get(i));
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
			else
			{
//				System.out.println("size of LRU is " + size);
				this.changeLRU(2, pid);
			}
			int fileId = pid.getTableId();
			DbFile file = Database.getCatalog().getDbFile(fileId);
			Page newpage = file.readPage(pid);
			//将新读入的page放入bufferpool
			pageMap.put(pid, newpage);
			return newpage;
		}
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid)
	{
		// some code goes here
		// not necessary for lab1|lab2
		lockManager.releaseLockOnPage(pid, tid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException
	{
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p)
	{
		// some code goes here
		// not necessary for lab1|lab2
		return lockManager.holdsLockOnPage(p, tid);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException
	{
		// some code goes here
		// not necessary for lab1|lab2
		if (commit)
		{
			this.flushAllPages();
			// 删除该事务对应的所有加在页上的锁
			lockManager.releaseAllLocksInOneTransaction(tid);
			lockManager.printInfo("Committed! Having released all the lock on the transaction: " + tid.toString(), true);
			lockManager.printLockMap();
		}
		// Abort
		else
		{
			// 查找BufferPool中与该事务相关的dirty页面， 不换出， 直接从磁盘读取原来的页面信息
			Iterator <Entry<PageId, Page>> iterRecover = pageMap.entrySet().iterator();
			while (iterRecover.hasNext())
			{
				Map.Entry<PageId, Page> entry = (Map.Entry<PageId, Page>)iterRecover.next();
				Page tempPage = entry.getValue();
				if (tempPage.isDirty() == tid)
				{
					PageId pid = entry.getKey();
					int fileId = pid.getTableId();
					DbFile file = Database.getCatalog().getDbFile(fileId);
					Page recoverPage = file.readPage(pid);
					//将新读入的page放入bufferpool
					pageMap.put(pid, recoverPage);
				}
			}
			// 删除该事务对应的所有加在页上的锁
			lockManager.releaseAllLocksInOneTransaction(tid);
			lockManager.printInfo("Aborted! Having released all the lock on the transaction: " + tid.toString(), true);
			lockManager.printLockMap();
		}
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to(Lock acquisition
	 * is not needed for lab2). May block if the lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have
	 * been dirtied so that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException
	{
		// some code goes here
		// not necessary for lab1
		// Table已经放在了目录结构中，而且得到的p里面已经有了插入的记录tuple
		PageId pid = null;
		DbFile file = Database.getCatalog().getDbFile(tableId);
		List<Page> pages = file.addTuple(tid, t);
		for (Page p : pages)
		{
			p.markDirty(true, tid);
			pid = p.getId();
			pageMap.put(pid, p);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from. May block if the lock cannot
	 * be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit. Does not need to update cached versions of any pages
	 * that have been dirtied, as it is not possible that a new page was created
	 * during the deletion (note difference from addTuple).
	 *
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException
	{
		// some code goes here
		// not necessary for lab1
		// 根据给出的Tuple求出相应的页号pid，从pid中得到对应的tableId，再查目录得到对应的DbFile
		// 事务结束的时候释放锁，而且是两阶段锁，统一释放
		PageId pid = t.getRecordId().getPageId();
		int tableId = pid.getTableId();
		DbFile file = Database.getCatalog().getDbFile(tableId);
		Page page = file.deleteTuple(tid, t);
		page.markDirty(true, tid);
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException
	{
		// some code goes here
		// not necessary for lab1
		// 使用迭代器遍历HashMap
		Iterator<Entry<PageId, Page>> it = this.pageMap.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry<PageId, Page> entry = (Map.Entry<PageId, Page>) it.next();
			PageId pid = entry.getKey();
			this.flushPage(pid);
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 */
	public synchronized void discardPage(PageId pid)
	{
		// some code goes here
		// only necessary for lab5
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	// 使用了BufferPool中的Map<PageId, Page> pageMap，
	// 所以一定是在BufferPool里面的Page才可以写出去，此时没有把BufferPool里面的page清除
	// 此时对应的事务ID已经从page.isDirty()中得到
	private synchronized void flushPage(PageId pid) throws IOException
	{
		// some code goes here
		// not necessary for lab1
		int tableId = pid.getTableId();
		HeapFile hpFile = (HeapFile) Database.getCatalog().getDbFile(tableId);
		Page page = this.pageMap.get(pid);
		TransactionId tid = page.isDirty();
		if (tid != null)
		{
			hpFile.writePage(page);
			page.markDirty(false, tid);
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException
	{
		// some code goes here
		// not necessary for lab1|lab2|lab3
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	// 使用LRU算法，把最近最少使用的页面换出去
	// 仅仅只有该方法能够从BufferPool里面删除页, 且synchronized
	private synchronized void evictPage() throws DbException
	{
		// some code goes here
		// not necessary for lab1
		int i = 0;
		int size = this.LRU.size();
		// 先判断是不是全部dirty, 是的话抛出DbException, 给getPage处理(仅仅有getPage调用过该方法)
		for (i = 1; i <= size; i++)
		{
			PageId trypid = this.LRU.get(size - i);
			Page page = pageMap.get(trypid);
			TransactionId tid = page.isDirty();
			if (tid == null)
			{
				break;
			}
		}
		if (i == size + 1)
		{
			throw new DbException("All pages in the buffer pool are dirty!");
		}
		i = 0;
		// 在No_Steal策略下删除一个clean but locked的页，比如原本LRU替换策略发现要换出最后那一页，
		// 但是那一页是dirty的，而且被Locked了，所以找另一个clean的页来换出去
		for (i = 1; i <= size; i++)
		{
			PageId trypid = this.LRU.get(size - i);
			Page page = pageMap.get(trypid);
			TransactionId tid = page.isDirty();
			// dirty的而且被某事务加锁且没释放的页，不能换出去
			if (tid != null && holdsLock(tid, trypid) == true)
			{
				continue;
			}
			// dirty的但是没有被加锁的可以换出，但是要写回到磁盘里
			else if (tid != null && holdsLock(tid, trypid) == false)
			{
				this.changeLRU(3, trypid);
				try {
					this.flushPage(trypid);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new DbException("IOException happen when flushPage in evict method!");
				}
			}
			else
			{
				this.changeLRU(3, trypid);
				break;
			}
		}
	}
	
	public static void isDeadLock(TransactionId tid) throws TransactionAbortedException
	{
		long currentTime = System.currentTimeMillis();
		enduranceTimeMap.putIfAbsent(tid, currentTime);
		long lastTimeTemp = enduranceTimeMap.get(tid);
		if (currentTime - lastTimeTemp > 100)
		{
			lockManager.printInfo("DeadLock and throw TransactionAbortedException", true);
			throw new TransactionAbortedException();
		}
	}
	
	/**
	 * 当condition == true的时候
	 * 打印出锁的信息
	 * 
	 * @param info
	 * @param condition
	 */
	private void printLockMap(String info, boolean condition)
	{
		// level2 printf all the info about locks
		lockManager.printInfo(info, condition);
		lockManager.printLockMap();
	}
	
	/**
	 * 改变LRU链表的状态, 并同步删除Map<PageId, Page> pageMap但是不同步加入;
	 * @param option
	 * 		option == 0的时候删除最少使用的元素(rear),再把现在访问的pid插入数组首部
	 * 		option == 1的时候将现在访问的pid删除后再插入数组首部
	 * 		option == 2的时候把现在访问的pid插入数组首部
	 * 		option == 3的时候把指定的pid删除
	 * 另外：LRULock是对凡是调用changeLRU的线程加锁，
	 * 以保证所有的线程互斥地修改LRU
	 * 仅仅有getPage（）方法使用了LRU换页策略
	 */
	private void changeLRU(int option, PageId pid)
	{
		LRULock.lock();
		int size = this.LRU.size();
		PageId removePid = null;
		switch(option)
		{
		case 0:
			{
				removePid = this.LRU.get(size - 1);
				this.LRU.remove(size - 1);
				this.LRU.add(0, pid); 
				this.pageMap.remove(removePid); 
				break;
			}
		case 1:this.LRU.remove((Object)pid); this.LRU.add(0, pid); break;
		case 2:this.LRU.add(0, pid); break;
		case 3:this.LRU.remove((Object)pid); this.pageMap.remove(pid); break;
		default: break;
		}
		LRULock.unlock();
	}
}



