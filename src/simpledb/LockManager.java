package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 负责管理所有页面上的锁，并记录事务，页面上的所有读写锁信息
 * 
 * @author qsct
 *
 */

public class LockManager {
	private final Map<PageId, Object> locksOnPage; // 保证申请的时候对页的互斥
	private final Map<PageId, TransactionId> exclusiveLockOnPage;
	private final Map<PageId, Collection<TransactionId>> shareLockOnPage;
	private final Map<TransactionId, Collection<PageId>> pageLockedByTransaction;
	
	public LockManager()
	{
		locksOnPage = new ConcurrentHashMap<PageId, Object>();
		exclusiveLockOnPage = new HashMap<PageId, TransactionId>();
		shareLockOnPage = new HashMap<PageId, Collection<TransactionId>>();
		pageLockedByTransaction = new ConcurrentHashMap<TransactionId, Collection<PageId>>();
	}
	
	/**
	 * 判断某事务是否已经持有了该页上的读权限
	 * 如果某事务已经持有了该页上的写权限则可以直接读
	 * 问题：会不会有同一事务申请同一个页面上的读锁的情况先后出现呢？
	 * 
	 * @param pid
	 * @param tid
	 * @return
	 * 		有读权限则返回True， 没有则false
	 */
	public boolean hasReadPermission(PageId pid, TransactionId tid)
	{
		if (exclusiveLockOnPage.containsKey(pid) && tid.equals(exclusiveLockOnPage.get(pid)))
		{
			return true;
		}
		// FIXME 程序会执行到这里吗？
		else if (shareLockOnPage.containsKey(pid) && shareLockOnPage.get(pid).contains(tid))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public boolean hasWritePermission(PageId pid, TransactionId tid)
	{
		if (exclusiveLockOnPage.containsKey(pid) && tid.equals(exclusiveLockOnPage.get(pid)))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * 某事务申请一个页面上的指定权限的锁
	 * 
	 * @param pid
	 * @param tid
	 * @param perm
	 * @return
	 * @throws TransactionAbortedException
	 * 		当申请锁无法通过的时候判断是否死锁来杀死事务
	 * 		如果死锁则抛出TransactionAbortedException
	 * 
	 * 整个申请和释放过程是：某事务开始->getPage->acquireLock->循环判断->isDeadLock->TransactionAbortedException->事务中调用getPage的方法捕获异常
	 * ->TransactionComplete（tid, false）-> Abort -> releaseLock 并修改tidToPid映射表
	 * 
	 */
	public boolean acquireLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException
	{
		// Only happened in EvictTest
		if (tid == null)
		{
			tid = new TransactionId();
		}
		printInfo("Before applying the ReadOnly lock", true);
		printLockMap();
		if (perm == Permissions.READ_ONLY)
		{
			printInfo("Try to fetch a ReadOnly lock!" + pid.toString() + " , " + tid.toString(), true);
			// 针对自身的情况判断，没必要加锁
			if (hasReadPermission(pid, tid))
			{
				printInfo("Already hold the permission lock!", true);
				printLockMap();
				return true;
			}
			while(!acquireReadOnlyLock(pid, tid, perm));
			printInfo("Getting readOnly lock successfully!", true);
			printLockMap();
		}
		else if (perm == Permissions.READ_WRITE)
		{
			printInfo("Try to fetch a ReadWrite lock!" + pid.toString() + " , " + tid.toString(), true);
			if (hasWritePermission(pid, tid))
			{
				printInfo("Already hold the READ_WRITE lock!", true);
				printLockMap();
				return true;
			}
			while(!acquireReadWriteLock(pid, tid, perm));
			printInfo("Getting readWrite lock successfully!", true);
			printLockMap();
		}
		else
		{
			// FIXME need to throw an error
			return false;
		}
		addTidToPids(pid, tid);
		return true;
	}
	
	/**
	 * 获得读锁后给读锁表增加信息
	 * @param pid
	 * @param tid
	 */
	public void addShareLock(PageId pid, TransactionId tid)
	{
		if (!shareLockOnPage.containsKey(pid))
		{
			ArrayList<TransactionId> tempShareArray = new ArrayList<TransactionId>();
			tempShareArray.add(tid);
			shareLockOnPage.put(pid, (Collection<TransactionId>) tempShareArray);
		}
		else
		{
			shareLockOnPage.get(pid).add(tid);
		}
	}
	
//	/**
//	 * 删除某事务tid上某pid的读锁
//	 * 
//	 * @param pid
//	 * @param tid
//	 */
//	public void removeShareLock(PageId pid, TransactionId tid)
//	{
//		if (shareLockOnPage.containsKey(pid))
//		{
//			Collection<TransactionId> tempShareArray = shareLockOnPage.get(pid);
//			tempShareArray.remove((Object)tid);
//			if (tempShareArray.isEmpty())
//			{
//				shareLockOnPage.remove(pid);
//			}
//		}
//		else
//		{
//			// FIXME throw an error
//			;
//		}
//	}
	
	/**
	 * 释放一个事务已经持有的所有锁
	 * 一般在Transaction complete or Transaction Abort的时候调用
	 * 注意：使用for-each遍历一个ArrayList而不是使用迭代器
	 * 		
	 * 出现的错误提示：NullPointer(153行) isDeadLock抛出的错误没函数去处理
	 * 			    为何在释放之前不要加互斥锁？
	 * @param tid
	 */
	public void releaseAllLocksInOneTransaction(TransactionId tid)
	{
		if (pageLockedByTransaction.containsKey(tid))
		{
			LinkedBlockingQueue<PageId> pidCollection = (LinkedBlockingQueue<PageId>) pageLockedByTransaction.get(tid);
			for (PageId tempPid : pidCollection)
			{
				printInfo("Null pointer and Tid is " + tid.toString() + " ; " + tempPid.toString(), tempPid == null);
				Object lock = getLock(tempPid);
//				synchronized (lock) 
//				{
					if (exclusiveLockOnPage.containsKey(tempPid))
					{
						exclusiveLockOnPage.remove(tempPid, tid);
					}
					else
					{
						removeReadOnlyLock(tempPid, tid);
					}
					removeTidAndPids(tempPid, tid);
//				}
			}
		}
	}
	
	/**
	 * 把指定的键值对加入到pageLockedByTransaction里面
	 * 显示出指定事务上有那些页加了锁
	 * 此过程保证了原子
	 * @param pid
	 * @param tid
	 */
	public void addTidToPids(PageId pid, TransactionId tid)
	{
		pageLockedByTransaction.putIfAbsent(tid, new LinkedBlockingQueue<PageId>());
		pageLockedByTransaction.get(tid).add(pid);
	}
	
	/**
	 * 删除ConcurrentHashMap<TransactionId, Collection<PageId>>里面
	 * 的一组键值对，指定键值对的pid和tid由参数传入
	 * 如果删除pid后的集合为空
	 * 则把tid和集合的键值对信息给删掉
	 * 
	 * @param pid
	 * @param tid
	 */
	public void removeTidAndPids(PageId pid, TransactionId tid)
	{
		LinkedBlockingQueue<PageId> temp = (LinkedBlockingQueue<PageId>) pageLockedByTransaction.get(tid);
		temp.remove((Object)pid);
		if (temp.isEmpty())
		{
			pageLockedByTransaction.remove(tid, temp);
		}
		else
		{
			pageLockedByTransaction.replace(tid, temp);
		}
	}
	
	/**
	 * 申请读锁， 成功则返回true
	 * 
	 * @param pid
	 * @param tid
	 * @param perm
	 * @return
	 * @throws TransactionAbortedException
	 */
	public boolean acquireReadOnlyLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException
	{
		Object lock = getLock(pid);
		synchronized (lock) 
		{
			while(true)
			{
				if (exclusiveLockOnPage.containsKey(pid) == false)
				{
					addShareLock(pid, tid);
					return true;
				}
				// FIXME 加入到依赖图中
				else
				{
					BufferPool.isDeadLock(tid);
				}
			}
		}
	}
	
	/**
	 * 申请写锁，升级操作在此实现
	 * 注意：只有在一次申请锁失败以后才调用isDeadLock()方法来检测是否死锁
	 * 
	 * @param pid
	 * @param tid
	 * @param perm
	 * @return
	 * @throws TransactionAbortedException 
	 */
	public boolean acquireReadWriteLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException
	{
		Object lock = getLock(pid);
		synchronized (lock) 
		{
			while(true)
			{
				if (exclusiveLockOnPage.containsKey(pid) == false && shareLockOnPage.containsKey(pid) == false)
				{
					exclusiveLockOnPage.put(pid, tid);
					return true;
				}
				// 锁升级操作的实现
				else if(exclusiveLockOnPage.containsKey(pid) == false && shareLockOnPage.containsKey(pid) == true
						&& shareLockOnPage.get(pid).size() == 1 && shareLockOnPage.get(pid).contains(tid))
				{
					removeReadOnlyLock(pid, tid);
					exclusiveLockOnPage.put(pid, tid);
					return true;
				}
				// FIXME 加入到依赖图中
				else
				{
					BufferPool.isDeadLock(tid);
				}
			}
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
	public void releaseLockOnPage(PageId pid, TransactionId tid)
	{
		if (exclusiveLockOnPage.containsKey(pid) && tid.equals(exclusiveLockOnPage.get(pid)))
		{
			exclusiveLockOnPage.remove(pid);
		}
		else if (shareLockOnPage.containsKey(pid) && shareLockOnPage.get(pid).contains(tid))
		{
			Collection<TransactionId> temp = shareLockOnPage.get(pid);
			temp.remove(tid);
			if (temp.isEmpty())
			{
				shareLockOnPage.remove(pid, temp);
			}
			else
			{
				shareLockOnPage.put(pid, temp);
			}
		}
	}
	
	/**
	 * 删除某事务tid上某pid的读锁
	 * 
	 * @param pid
	 * @param tid
	 */
	public void removeReadOnlyLock(PageId pid, TransactionId tid)
	{
		if (shareLockOnPage.containsKey(pid) && shareLockOnPage.get(pid).contains(tid))
		{
			Collection<TransactionId> temp = shareLockOnPage.get(pid);
			temp.remove(tid);
			if (temp.isEmpty())
			{
				shareLockOnPage.remove(pid, temp);
			}
			else
			{
				shareLockOnPage.put(pid, temp);
			}
		}
		// FIXME throw an error
		else
		{
			;
		}
	}
	
	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLockOnPage(PageId pid, TransactionId tid)
	{
		if (exclusiveLockOnPage.containsKey(pid))
		{
			if (exclusiveLockOnPage.get(pid).equals(tid))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else if (shareLockOnPage.containsKey(pid))
		{
			if (shareLockOnPage.get(pid).contains(tid))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * 获得某页面上的锁，保证互斥
	 * 
	 * @param pid
	 * @return
	 */
	public Object getLock(PageId pid)
	{
		locksOnPage.putIfAbsent(pid, new Object());
		return locksOnPage.get(pid);
	}
	
	public void printInfo(String str, boolean condition)
	{
		if (condition == true)
		{
			if (Debug.isEnabled(2))
			{
				Debug.log(str);
			}
		}
	}
	
	public void printLockMap()
	{
		printExclusiveLock();
		printshareLock();
		printPageLockedByTransaction();
	}
	
	public void printExclusiveLock()
	{
		Iterator<Entry<PageId, TransactionId>> iter = exclusiveLockOnPage.entrySet().iterator();
		if (Debug.isEnabled(4))
		{
			Debug.log("exclusiveLockOnPage : ");
		}
		while (iter.hasNext())
		{
			Map.Entry<PageId, TransactionId> entry = (Map.Entry<PageId, TransactionId>)iter.next();
			PageId tempPid = entry.getKey();
			TransactionId tempTid = entry.getValue();
			if (Debug.isEnabled(4))
			{
				Debug.log(tempPid.toString() + " ; " + tempTid.toString() + " ; ");
			}
		}
	}
	
	public void printshareLock()
	{
		Iterator<Entry<PageId, Collection<TransactionId>>> iter = shareLockOnPage.entrySet().iterator();
		if (Debug.isEnabled(4))
		{
			Debug.log("shareLockOnPage : " + "PageId and its TransactionIds");
		}
		while (iter.hasNext())
		{
			Map.Entry<PageId, Collection<TransactionId>> entry = (Map.Entry<PageId, Collection<TransactionId>>)iter.next();
			PageId tempPid = entry.getKey();
			ArrayList<TransactionId> tempTidArray = (ArrayList<TransactionId>) entry.getValue();
			if (Debug.isEnabled(4))
			{
				Debug.log(tempPid.toString());
				Iterator<TransactionId> it = tempTidArray.iterator();
				while (it.hasNext())
				{
					TransactionId tempTid = it.next();
					Debug.log(tempTid.toString());
				}
			}
		}
	}
	
	public void printPageLockedByTransaction()
	{
		Iterator<Entry<TransactionId, Collection<PageId>>> iter = pageLockedByTransaction.entrySet().iterator();
		if (Debug.isEnabled(4))
		{
			Debug.log("pageLockedByTransaction : " + "TransactionId and its PageIds");
		}
		while (iter.hasNext())
		{
			Map.Entry<TransactionId, Collection<PageId>> entry = (Map.Entry<TransactionId, Collection<PageId>>)iter.next();
			TransactionId tempTid = entry.getKey();
			LinkedBlockingQueue<PageId> tempPidQueue = (LinkedBlockingQueue<PageId>) entry.getValue();
			if (Debug.isEnabled(4))
			{
				Debug.log(tempTid.toString());
				Iterator<PageId> it = tempPidQueue.iterator();
				while (it.hasNext())
				{
					PageId tempPid = it.next();
					Debug.log(tempPid.toString());
				}
			}
		}
		if (Debug.isEnabled(4))
		{
			Debug.log("--------------------------");
		}
	}
}
