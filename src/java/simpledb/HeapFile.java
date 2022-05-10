package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file=f;
        tupleDesc=td;
    }
    private File file;
    private TupleDesc tupleDesc;
    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        int id;
        id=file.getAbsoluteFile().hashCode();
        return id;
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;

        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page res=null;
        byte[] page_data=new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile randomAccessFile=new RandomAccessFile(getFile(),"r");
            int st=pid.getPageNumber()*BufferPool.getPageSize();
            randomAccessFile.seek(st);
            randomAccessFile.read(page_data,0,BufferPool.getPageSize());
            res=new HeapPage((HeapPageId) pid,page_data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
        //return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here

        int pageno=page.getId().getPageNumber();

        RandomAccessFile randomAccessFile=new RandomAccessFile(getFile(),"rw");
        int st=pageno*BufferPool.getPageSize();
        randomAccessFile.seek(st);
        randomAccessFile.write(page.getPageData());
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)file.length()/BufferPool.getPageSize();

        //return 0;
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages=new ArrayList<>();
        for(int i=0;i<numPages();i++){
            //HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),i),Permissions.READ_WRITE);
            HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),i),Permissions.READ_ONLY);
            if(heapPage.getNumEmptySlots()==0)
            {
                //该页满了，释放在该页上的锁
                Database.getBufferPool().releasePage(tid,heapPage.getId());
                continue;
            }
            Database.getBufferPool().releasePage(tid,heapPage.getId());

            //对有空余的页申请exclusive锁
            heapPage=(HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),i),Permissions.READ_WRITE);
            heapPage.insertTuple(t);

            pages.add(heapPage);
            return pages;
        }

        BufferedOutputStream bufferedOutputStream=new BufferedOutputStream(new FileOutputStream(file,true));
        bufferedOutputStream.write(HeapPage.createEmptyPageData());
        bufferedOutputStream.close();

        HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        pages.add(heapPage);
        return pages;

       // return null;
        // not necessary for lab1
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        PageId pageId=t.getRecordId().getPageId();
        HeapPage heapPage=(HeapPage)Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
        heapPage.deleteTuple(t);

        ArrayList<Page> pages=new ArrayList<>();
        pages.add(heapPage);

        return pages;

        //return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this, tid);
    }
    public static class HeapFileIterator implements DbFileIterator{

        HeapFile heapFile;
        TransactionId transactionId;
        int currentPage;
        Iterator<Tuple> tupleIterator;

        HeapFileIterator(HeapFile file,TransactionId tid){
            heapFile=file;
            transactionId=tid;
        }

        public Iterator<Tuple> getTupleIterator(int page) throws ArrayIndexOutOfBoundsException, TransactionAbortedException, DbException {
            if(page<0||page>=heapFile.numPages())
                throw new ArrayIndexOutOfBoundsException();

            HeapPageId pageId=new HeapPageId(heapFile.getId(),currentPage);
            return ((HeapPage)(Database.getBufferPool().getPage(transactionId,pageId,Permissions.READ_ONLY))).iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPage=0;
            tupleIterator=this.getTupleIterator(currentPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator==null)
                return false;
            if(tupleIterator.hasNext())
                return true;
            else
            {
                if(currentPage< heapFile.numPages()-1)
                {
                    currentPage++;
                    tupleIterator=this.getTupleIterator(currentPage);
                    return tupleIterator.hasNext();
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            //return null;
            if(tupleIterator==null)
                throw new NoSuchElementException("null pointer");

            if(tupleIterator.hasNext())
                return tupleIterator.next();
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }
        @Override
        public void close() {
            tupleIterator=null;
        }
    }
}

