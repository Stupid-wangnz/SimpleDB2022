package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages=numPages;
        pageHashMap=new ConcurrentHashMap<>(numPages);

        pageLockManager=new PageLockManager();
    }


    private ConcurrentHashMap<PageId,Page> pageHashMap;
    private int numPages;
    PageLockManager pageLockManager;

    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private class PageLock{
        TransactionId tid;
        int lockType;//0 is shared lock, 1 is exclusive lock
        public PageLock(TransactionId tid, int lockType){
            this.tid=tid;
            this.lockType=lockType;
        }
    }

    private class PageLockManager {
        ConcurrentHashMap<PageId, ArrayList<PageLock>> pageLocks;
        /*依赖图，用来判断是否有环出现死锁
        依赖图的逻辑:每个结点就是hashmap里的key，对应的value是key指向的其他结点的集合
        ...单向边的集合可以理解为
         */
        ConcurrentHashMap<TransactionId, ArrayList<TransactionId>> dependencyMap;

        public PageLockManager() {
            pageLocks = new ConcurrentHashMap<>();
            dependencyMap = new ConcurrentHashMap<>();
        }

        public synchronized void addPageLock(PageId pid, PageLock pl) {
            if (pageLocks.containsKey(pid)) {
                pageLocks.get(pid).add(pl);
            } else {
                ArrayList<PageLock> pls = new ArrayList<>();
                pls.add(pl);
                pageLocks.put(pid, pls);
            }
            removeDependency(pl.tid);
        }

        public synchronized void addDependency(TransactionId tid1, TransactionId tid2) {
            if (dependencyMap.containsKey(tid1)) {
                dependencyMap.get(tid1).add(tid2);
            } else {
                ArrayList<TransactionId> tids = new ArrayList<>();
                tids.add(tid2);
                dependencyMap.put(tid1, tids);
            }
        }

        public synchronized void removeDependency(TransactionId tid) {
            dependencyMap.remove(tid);
        }

        public synchronized boolean hasCycle(TransactionId tid) {
            //判断dependencyMap是否有环
            //如果有回路，则返回true
            //如果没有回路，则返回false
            ArrayList<TransactionId> tids = dependencyMap.get(tid);
            if (tids == null) {
                return false;
            }
            //来判断是否有环
            //如果有环，则返回true
            //如果没有环，则返回false
            //通过tupo判断是否有环

            //记录入度
            ConcurrentHashMap<TransactionId,Integer> dependency_in=new ConcurrentHashMap<>();
            ConcurrentLinkedQueue<TransactionId>queueList=new ConcurrentLinkedQueue<>();
            queueList.add(tid);
            dependency_in.put(tid,0);

            //加载入度图
            while(!queueList.isEmpty()){
                TransactionId cur=queueList.remove();

                ArrayList<TransactionId> ntids=dependencyMap.get(cur);

                if(ntids==null)
                    continue;

                for(TransactionId ntid:ntids){
                    //if ntid has in dependdency_in map, meaning it was visited
                    if(dependency_in.containsKey(ntid)){
                        int t=dependency_in.get(ntid);
                        t++;
                        dependency_in.replace(ntid,t);
                        continue;
                    }
                    queueList.add(ntid);
                    dependency_in.put(ntid,1);
                }
            }

            //拓扑排序，判断环
            while(true){
                int count=0;
                for(TransactionId nexttid:dependency_in.keySet()){
                    if(dependency_in.get(tid)==null)
                        continue;
                    //find in 0 的节点
                    if(dependency_in.get(tid)==0){
                        ArrayList<TransactionId>totids=dependencyMap.get(nexttid);
                        if(totids==null)
                            continue;
                        //所以邻接节点入度-1
                        for(TransactionId totid:totids){
                            int t=dependency_in.get(totid);
                            t--;
                            dependency_in.put(totid,t);
                        }
                        dependency_in.remove(nexttid);
                        count++;
                    }
                }
                if(count==0)
                    break;
            }

            return !dependency_in.isEmpty();
        }

        private synchronized boolean dfs(TransactionId tid,HashSet<TransactionId>visited,HashSet<TransactionId>visiting) {
            visiting.add(tid);
            ArrayList<TransactionId> tids = dependencyMap.get(tid);
            if(tids==null) {
                visiting.remove(tid);
                visited.add(tid);
                return false;
            }
            for(TransactionId ntid:tids){
                if(visiting.contains(ntid))
                    return true;
                if(visited.contains(ntid))
                    continue;

                if(dfs(ntid,visited,visiting))
                    return true;
            }
            visiting.remove(tid);
            visited.add(tid);
            return false;
        }


        //tid在申请读取pid之前判断，是否能够申请锁
        public synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType) {
            //申请读取pid，如果pid上有其他事务的写锁，则返回false
            if (!pageLocks.containsKey(pid)) {
                //no locks on this page,put new lock on it and return true
                addPageLock(pid, new PageLock(tid, lockType));
                return true;
            }
            /*
            如果pageLocks中已经有了这个pid，那么就要判断这个pid已经被哪些锁锁住了
             */
            ArrayList<PageLock> locks = pageLocks.get(pid);

            for (PageLock lock : locks) {
                if (lock.tid.equals(tid)) {
                    //如果这个锁是自己的
                    //0-0 1-0 1-1的情况
                    if (lock.lockType == lockType||lock.lockType==1) {
                        //如果这个锁是自己的，并且是想要的锁类型，那么返回true
                        return true;
                    }
                    //0-1的情况
                    //如果已有的锁是读锁，require的是写锁，且pid上只有一把锁，那么将这个锁改为写锁
                    if (locks.size() == 1) {
                        lock.lockType = 1;
                        return true;
                    }

                }
            }
            //如果没有找到自己的锁，pid上的锁是不是其他事务的写锁，如果是，那么返回false
            if (locks.size()==1&&locks.get(0).lockType == 1) {
                addDependency(tid,locks.get(0).tid);
                return false;
            }
            //剩下的情况是pid上的锁是其他事务的读锁，那如果是想要的读锁，那么返回true
            if (lockType == 0) {
                addPageLock(pid, new PageLock(tid, lockType));
                return true;
            }

            //剩下的情况就是，pid上的锁是别的事务的读锁，但tid申请写锁
            for(PageLock lock:locks){
                addDependency(tid,lock.tid);
            }
            return false;
        }

        public synchronized void releaseLock(PageId pid, TransactionId tid) {
            //release the lock by tid on pid
            if (pageLocks.containsKey(pid)) {
                ArrayList<PageLock> locks = pageLocks.get(pid);
                for (PageLock lock : locks) {
                    if (lock.tid.equals(tid)) {
                        locks.remove(lock);
                        if (locks.size() == 0) {
                            pageLocks.remove(pid);
                        }
                        return;
                    }
                }
            }
        }

        public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
            //check if tid holds the lock on pid
            if (pageLocks.containsKey(pid)) {
                ArrayList<PageLock> locks = pageLocks.get(pid);
                for (PageLock lock : locks) {
                    if (lock.tid.equals(tid)) {
                        return true;
                    }
                }

            }
            return false;
        }
    }
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        //tid在访问pid时锁的类型
        int lockType=perm==Permissions.READ_ONLY?0:1;

        //如果tid已经获得了pid的锁，那么直接返回
        boolean canGetLock=pageLockManager.acquireLock(pid, tid, lockType);


        while (!canGetLock){
            //如果tid没有获得pid的锁，那么就要等待
            try {
                //wait();
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(pageLockManager.hasCycle(tid)) {
                //System.out.println(tid+" has deadlock");
                throw new TransactionAbortedException();
            }
            canGetLock=pageLockManager.acquireLock(pid, tid, lockType);
        }
        /*超时策略判断死锁*/
        /*long startTime=System.currentTimeMillis();
        while (!canGetLock){
            //如果tid没有获得pid的锁，那么就要等待

            if(System.currentTimeMillis()-startTime>=1500){
                //如果15s没获得锁，就是超时了，那么抛出死锁异常，由上层程序捕获并回滚
                throw new TransactionAbortedException();
            }
            try {
                //wait();
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            canGetLock=pageLockManager.acquireLock(pid, tid, lockType);
        }*/

        //System.out.println(tid+"success get lock");

        if(pageHashMap.size()>=numPages)
            evictPage();

        if(!pageHashMap.containsKey(pid)){
            DbFile dbFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page= dbFile.readPage(pid);
            pageHashMap.put(pid,page);
        }

        return pageHashMap.get(pid);
        //return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        pageLockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2

        return pageLockManager.holdsLock(p, tid);
        //return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        if(commit){
            flushPages(tid);
        }
        else{
            rollbackPages(tid);
        }

        for(PageId pid:pageHashMap.keySet()){
            if(pageLockManager.holdsLock(pid, tid)){
                pageLockManager.releaseLock(pid, tid);
            }
        }
    }


    private synchronized void rollbackPages(TransactionId tid) throws IOException {
        //事务tid中断，回滚tid中的所有页面
        for(PageId pid:pageHashMap.keySet()){
            if(pageLockManager.holdsLock(pid, tid)){
                DbFile dbFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page page=dbFile.readPage(pid);
                pageHashMap.replace(pid,page);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        DbFile dbFile=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page>pages=dbFile.insertTuple(tid,t);
        for(int i=0;i<pages.size();i++){
            pages.get(i).markDirty(true,tid);
            pageHashMap.put(pages.get(i).getId(),pages.get(i));
        }

        // not necessary for lab1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int tableId=t.getRecordId().getPageId().getTableId();
        DbFile dbFile=Database.getCatalog().getDatabaseFile(tableId);
        //System.out.println(t);
        ArrayList<Page>pages=dbFile.deleteTuple(tid,t);
        for(int i=0;i<pages.size();i++){
            pages.get(i).markDirty(true,tid);
            pageHashMap.put(pages.get(i).getId(),pages.get(i));
        }
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

        for(Page page:pageHashMap.values()){
            if(page.isDirty()!=null){
                flushPage(page.getId());
            }
        }


    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        pageHashMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page page=pageHashMap.get(pid);
        if(page.isDirty()==null)
            return;

        DbFile dbFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false,null);

        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        for(Page page:pageHashMap.values()){
            if(page.isDirty()!=null&&page.isDirty().equals(tid)){
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page removedPage=null;
        for(Page page:pageHashMap.values()){
            if(page.isDirty()==null){
                removedPage=page;
                break;
            }
        }

        if(removedPage==null)
            throw new DbException("all dirty page");
        discardPage(removedPage.getId());
    }
}

