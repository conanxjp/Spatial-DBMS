package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.RectKey;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.Rect;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import sqlinterface.Parser;
import sqlinterface.TableInfo;

class Part3WriteDriver extends TestDriver implements GlobalConst
{
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	
	private String createTableQuery[] = new String[2];
	private String insertQuery1[] = new String[10];
	private String insertQuery2[] = new String[5];
	private RID ridList1[] = new RID[10];
	private RID ridList2[] = new RID[5];
	private Heapfile f = null;	
	

	public Part3WriteDriver ()
	{
		super("part3_");
		//dbpath = "/Users/conanxjp/Desktop/dbtests/"+"part3test.minibase-db";
	}
	
	protected String testName () 
	{
		return "Part 3 Write Test";
	}
	
	public boolean runTest()
	{
		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
		
		// Kill anything that might be hanging around
		    String newdbpath;
		    String newlogpath;
		    String remove_logcmd;
		    String remove_dbcmd;
		    String remove_cmd = "/bin/rm -rf ";
		    
		    newdbpath = dbpath;
		    newlogpath = logpath;
		    
		    remove_logcmd = remove_cmd + logpath;
		    remove_dbcmd = remove_cmd + dbpath;
		    
		    // Commands here is very machine dependent.  We assume
		    // user are on UNIX system here
		    
		    try {
		      Runtime.getRuntime().exec(remove_logcmd);
		      Runtime.getRuntime().exec(remove_dbcmd);
		    }
		    catch (IOException e) {
		      System.err.println ("IO error: "+e);
		    }
		
		    SystemDefs sysdef = new SystemDefs(dbpath,4096,1000,"Clock");
		
		    //put catalog index filename and field type into hashmap
		    //catalog index field name list
		    ArrayList<String> catalogIndexFieldList = new ArrayList<String>();
		    catalogIndexFieldList.add("tableName");
		    catalogIndexFieldList.add("fieldName");
		    catalogIndexFieldList.add("fieldType");
		    TableInfo.addTable("catalogTable", catalogIndexFieldList);
		 
		    //catalog index attribute list value
		    ArrayList<AttrType> catalogIndexFieldAttrList = new ArrayList<AttrType>();
		    catalogIndexFieldAttrList.add(new AttrType(AttrType.attrString));
		    catalogIndexFieldAttrList.add(new AttrType(AttrType.attrString));
		    catalogIndexFieldAttrList.add(new AttrType(AttrType.attrInteger));
		    
		    //add to hash map
		    TableInfo.addAttrName(catalogIndexFieldList, catalogIndexFieldAttrList);
		    
		    Heapfile catfile = null;
			try {
				//System.out.println(tableName);
				catfile = new Heapfile("CatalogTableIndex");
				} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    String tableName[] = new String[2];
		    tableName[0] = "shopping_center";
		    tableName[1] = "sale_region";

		    String schema[] = new String[2];
		    schema[0] = "( id integer primary key, name string, shape rect );";
		    schema[1] = "( sid integer primary key, sname string, region rect );";
		    
		    for (int i = 0; i < 2; i ++)
		    {
			    createTableQuery[i] = "create table " + tableName[i] + " " + schema[i];
		    }
		   
		    String mallName[] = new String[10];
		    mallName[0] = "Mills";
		    mallName[1] = "Fiesta";
		    mallName[2] = "Fashion";
		    mallName[3] = "Lily";
		    mallName[4] = "Mehong";
		    mallName[5] = "Walmart";
		    mallName[6] = "Target";
		    mallName[7] = "Bashas";
		    mallName[8] = "Sprouts";
		    mallName[9] = "Frys";
		    
		    String rectValue = "";
			
		    int idNum[]=new int[10];
		    for(int i = 0; i < 10; i ++) 
		    {
			    idNum[i] = i;
		    }
		    Random ran=new Random();
		    int random;
		    int tmp;
		    for(int i = 0 ; i < 10; i ++) 
		    {
			    random=(ran.nextInt())%10;
			    if (random<0) random=-random;
			    tmp = idNum[i];
			    idNum[i] = idNum[random];
			    idNum[random] = tmp;
		    }
			
		    for (int i = 0; i < 10; i ++)
		    {
			    rectValue = "";
			    double x1 = (ran.nextDouble() * 10);
			    double y1 = ran.nextDouble() * 10;
			    double x2 = x1 + ran.nextDouble() * 10;
			    double y2 = y1 + ran.nextDouble() * 10;
			    DecimalFormat df = new DecimalFormat("#.00");
			    String strX1 = df.format(x1);
			    String strY1 = df.format(y1);
			    String strX2 = df.format(x2);
			    String strY2 = df.format(y2);
			    rectValue = strX1 + ", " + strY1 + ", " + strX2 + ", " + strY2;
			    insertQuery1[i] = "insert into shopping_center ( id, name, shape ) values ( " + (idNum[i] + 1) + ", " + mallName[i] + ", ( " + rectValue + " ) );";
		    }
		    
		    String saleRegionName[] = new String[5];
		    saleRegionName[0] = "NorthWest";
		    saleRegionName[1] = "NorthEast";
		    saleRegionName[2] = "SouthWest";
		    saleRegionName[3] = "SouthEest";
		    saleRegionName[4] = "Center";
		    
		    int saleRegionId[] = new int[5];
		    saleRegionId[0] = 101;
		    saleRegionId[1] = 102;
		    saleRegionId[2] = 201;
		    saleRegionId[3] = 201;
		    saleRegionId[4] = 111;
		    
		    String salePerson[] = new String[5];
		    salePerson[0] = "Alice";
		    salePerson[1] = "Bob";
		    salePerson[2] = "Carol";
		    salePerson[3] = "David";
		    salePerson[4] = "Eston";
		    
		    String region[] = new String[5];
		    region[0] = "0, 10, 10, 20";
		    region[1] = "10, 10, 20, 20";
		    region[2] = "0, 0, 10, 20";
		    region[3] = "10, 0, 20, 20";
		    region[4] = "5, 5, 15, 15";
		    
		    for (int i = 0; i < 5; i ++)
		    {
			    insertQuery2[i] = "insert into sale_region ( sid, sname, region ) values ( " 
					    		+ saleRegionId[i] + ", " + saleRegionName[i] + ", ( " + region[i] + " ) );";
		    }
		    

		    //Run the tests. Return type different from C++
		    boolean _pass = runAllTests();
		   
		    System.out.print ("\n" + "..." + testName() + " tests ");
		    System.out.print (_pass==OK ? "completely successfully" : "failed");
		    System.out.print (".\n\n");
		        
		    return _pass;
	}

	protected boolean test1 ()
	{
		System.out.println("\n-----------------------------------------------------Test1---------------------------------------------------------");
		System.out.println("\nTest saving the db file to disk ...\nCreate two tables and insert some records into them ...\n");
		boolean status = OK;
		for (int i  = 0; i < 2; i ++)
		{
			System.out.println("Input Query is: " + createTableQuery[i]);
			int parseResult = Parser.parseQuery(createTableQuery[i]);
			if (parseResult < 0)
			{
				System.err.println("Parsing the create table query fails");
				return status = FAIL;
			}
			System.out.println("Parsed Query is: " + Parser.rebuiltQuery());
			int numofRec = 0;
			f = null;
			try {
				f = new Heapfile("shopping_center");
			} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				status = FAIL;
				System.err.println("No table named shopping_center is found.");
				e.printStackTrace();
			}
			try {
				numofRec = f.getRecCnt();
			} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
					| IOException e) {
				// TODO Auto-generated catch block
				status = FAIL;
				System.err.println("Can't get record count from the heap file.");
				e.printStackTrace();
			}
			if (numofRec != 0) {
				status = FAIL;
				System.err.println("The table is not empty!");
			}
		}
		
		if (status)
			System.out.println("Create Table is successful.");
		else
		{
			status = FAIL;
			System.err.println("Create Table fails.");
		}
			
		if (status) {
			
			System.out.println("\n Inserting some records into the shopping_center table ...\n");
			
			for (int i = 0; i < 10; i ++)
			{
				System.out.println("Input Query is: " + insertQuery1[i]);
				int parseResult = Parser.parseQuery(insertQuery1[i]);
				ridList1[i] = Parser.rid;
				if (parseResult < 0)
				{
					System.out.println("Parsing the insert into query fails");
					return status = FAIL;
				}
				System.out.println("Parsed query is: " + Parser.rebuiltQuery());	
			}
			System.out.println("Test the record count in the table is 10 ...");
			if (status)
			{	
				int recCount = 0;
				try {
					f = new Heapfile("shopping_center");
				} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					System.err.println("No table named shopping_center is found.");
					e.printStackTrace();
				}	
				
				try {
					recCount = f.getRecCnt();
				} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
						| IOException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					System.err.println("Can't get record count from the heap file.");
					e.printStackTrace();
				}
				if (status)
				{
					if (recCount != 10)
					{
						status = FAIL;
						System.err.println("Insert 10 records into the table, but only see " + recCount);
					}
				}
				else
				{
					System.err.println("Insert fails");
					return status;
				}		
			}
			else
			{
				System.err.println("Insert fails");
				return status;
			}
			System.out.println("Pass.");
			System.out.println("Printing all records in table \"shopping_center\" ...");
			if (status)
			{
				for (int i = 0; i < 10; i ++)
				{			
					Tuple tuple;
					try {
						tuple = f.getRecord(ridList1[i]);
						ArrayList<String> attrNameList = TableInfo.tblInfo.get("shopping_center");
						int attrCnt = attrNameList.size();
						AttrType attrTypeList[] = new AttrType[attrCnt];
						for (int j = 0; j < attrCnt; j ++)
						{
							attrTypeList[j] = TableInfo.attrInfo.get(attrNameList.get(j));
						}
						tuple.print(attrTypeList);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						status = FAIL;
						e.printStackTrace();
					}
				}		
			}
			else
			{
				System.err.println("Insert fails");
				return status;
			}
			System.out.println("\nInsert some records into the sale_region table ...\n");
			for (int i = 0; i < 5; i ++)
			{
				System.out.println("Input Query is: " + insertQuery2[i]);
				int parseResult = Parser.parseQuery(insertQuery2[i]);
				ridList2[i] = Parser.rid;
				if (parseResult < 0)
				{
					System.err.println("Parsing the insert into query fails");
					return status = FAIL;
				}
				System.out.println("Parsed query is: " + Parser.rebuiltQuery());	
			}
			System.out.println("Test the record count in the table is 5 ...");
			if (status)
			{	
				int recCount = 0;
				try {
					f = new Heapfile("sale_region");
				} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					System.err.println("No table named sale_region is found.");
					e.printStackTrace();
				}	
				
				try {
					recCount = f.getRecCnt();
				} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
						| IOException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					System.err.println("Can't get record count from the heap file.");
					e.printStackTrace();
				}
				if (status)
				{
					if (recCount != 5)
					{
						status = FAIL;
						System.err.println("Insert 5 records into the table, but only see " + recCount);
					}
				}
				else
				{
					System.err.println("Insert fails");
					return status;
				}		
			}
			else
			{
				System.err.println("Insert fails");
				return status;
			}
			System.out.println("Pass.");
			System.out.println("Printing all records in table \"sale_region\" ...");
			if (status)
			{
				for (int i = 0; i < 5; i ++)
				{			
					Tuple tuple;
					try {
						tuple = f.getRecord(ridList2[i]);
						ArrayList<String> attrNameList = TableInfo.tblInfo.get("sale_region");
						int attrCnt = attrNameList.size();
						AttrType attrTypeList[] = new AttrType[attrCnt];
						for (int j = 0; j < attrCnt; j ++)
						{
							attrTypeList[j] = TableInfo.attrInfo.get(attrNameList.get(j));
						}
						tuple.print(attrTypeList);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						status = FAIL;
						e.printStackTrace();
					}
				}		
			}
			else
			{
				System.err.println("Insert fails");
				return status;
			}
			if (status)
				System.out.println("Insert is successful.");
			else
			{
				status = FAIL;
				System.err.println("Insert fails");
			}
			try {
				SystemDefs.JavabaseBM.flushAllPages();
			} catch (HashOperationException | PageUnpinnedException | PagePinnedException | PageNotFoundException
					| BufMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				SystemDefs.JavabaseDB.closeDB();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			  RandomAccessFile fp = null;
				try {
					fp = new RandomAccessFile(dbpath, "rw");
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 PageId pageid = new PageId();
				 pageid.pid = 0;
				 try {
					fp.seek((long)(pageid.pid *MINIBASE_PAGESIZE));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 Page bpage = new Page();
				 try {
					fp.read(bpage.getpage());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    
			
			// check db file exist on the disk
			File dbFile = new File (dbpath);
			boolean isExist = dbFile.exists();
			
			if (!isExist) {
				status = FAIL;
				System.err.println("Saving DB file to disk fails!");
			}
			else
				System.out.println("Saving DB file to disk successes!");
		}	
		return status;
	}
	
	protected boolean test2 ()
	{	
		return true;
	}
	
	protected boolean test3 ()
	{	
		return true;
	}
	
	protected boolean test4 ()
	{
		return true;
	}
	
	protected boolean test5 ()
	{
		return true;
	}
	
	protected boolean test6 ()
	{
		return true;
	}
}

public class Part3WriteTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Part3WriteDriver part3WriteDriver = new Part3WriteDriver();
		boolean part3Status = part3WriteDriver.runTest();
		if (!part3Status)
		{
			System.err.println ("Error encountered during " + part3WriteDriver.testName() + " tests:\n");
			 Runtime.getRuntime().exit(1);
		}
		
		Runtime.getRuntime().exit(0);
	}
}
