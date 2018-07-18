package tests;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.Rect;
import global.SystemDefs;
import heap.*;
import sqlinterface.InsertInto;
import sqlinterface.Parser;
import sqlinterface.TableInfo;

import java.io.IOException;
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
import btree.LeafData;
import btree.PinPageException;
import btree.RectKey;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;
import chainexception.*;

class SDODriver extends TestDriver implements GlobalConst
{
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	
	private String createTableQuery[] = new String[5];
	private String insertQuery1[] = new String[10];
	private String insertQuery2[] = new String[10];
	private String createIndexQuery[] = new String[5];
	private RID ridList1[] = new RID[10];
	private RID ridList2[] = new RID[5];
	private Heapfile f = null;	
	

	public SDODriver ()
	{
		super("sdotest");
		//dbpath = "/Users/conanxjp/Desktop/dbtests/"+"sdotest"+System.getProperty("user.name")+".minibase-db";
	}
	
	protected String testName () 
	{
		return "Spatial Geomeotry SQL";
	}
	
	public boolean runTest()
	{
		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
		
		SystemDefs sysdef = new SystemDefs(dbpath,8192,1000,"Clock");
		
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
		    
		    remove_logcmd = remove_cmd + newlogpath;
		    remove_dbcmd = remove_cmd + newdbpath;
		    
		    try {
		      Runtime.getRuntime().exec(remove_logcmd);
		      Runtime.getRuntime().exec(remove_dbcmd);
		    }
		    catch (IOException e) {
		      System.err.println ("IO error: "+e);
		    }
		    
		    String tableName[] = new String[5];
		    tableName[0] = "shopping_center";
		    tableName[1] = "sale_region";
		    tableName[2] = "sale_region_tempe";
		    tableName[3] = "mall_in_phoenix";
		    tableName[4] = "headquarter";
		    String schema[] = new String[5];
		    schema[0] = "( id integer primary key, name string, shape rect );";
		    schema[1] = "( sid integer primary key, sname string, region rect );";
		    schema[2] = "( sname string primary key, saleperson string, saleid integer, region rect );";
		    schema[3] = "( id integer, name string, shape rect );";
		    schema[4] = "( id integer primary key, name string, shape rect, location rect);";
		    
		    for (int i = 0; i < 5; i ++)
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
		    
		    //Clean up again
		    try {
		      Runtime.getRuntime().exec(remove_logcmd);
		      Runtime.getRuntime().exec(remove_dbcmd);
		    }
		    catch (IOException e) {
		      System.err.println ("IO error: "+e);
		    }
		    
		    System.out.print ("\n" + "..." + testName() + " tests ");
		    System.out.print (_pass==OK ? "completely successfully" : "failed");
		    System.out.print (".\n\n");
		    
		    return _pass;
	}

	protected boolean test1 ()
	{
		System.out.println("\n-----------------------------------------------------Test1---------------------------------------------------------");
		System.out.println("\nTesting Create Table query ...\n");
		boolean status = OK;
		for (int i  = 0; i < 5; i ++)
		{
			System.out.println("Input Query is: " + createTableQuery[i]);
			int parseResult = Parser.parseQuery(createTableQuery[i]);
			if (parseResult < 0)
			{
				System.out.println("Parsing the create table query fails");
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
				System.out.println("No table named shopping_center is found.");
				e.printStackTrace();
			}
			try {
				numofRec = f.getRecCnt();
			} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
					| IOException e) {
				// TODO Auto-generated catch block
				status = FAIL;
				System.out.println("Can't get record count from the heap file.");
				e.printStackTrace();
			}
			if (numofRec != 0) {
				status = FAIL;
				System.out.println("The table is not empty!");
			}
		}
		
		if (status)
			System.out.println("Create Table query test is successful.");
		else
			System.out.println("Create Table query test fails.");
		
		return status;
	}
	
	protected boolean test2 ()
	{
		System.out.println("\n-----------------------------------------------------Test2---------------------------------------------------------");
		System.out.println("\nTesting the Insert Into query ...\nInsert into 10 records into the table \"shopping_center\" created in test1.\n");
		boolean status = OK;
		
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
				System.out.println("No table named shopping_center is found.");
				e.printStackTrace();
			}	
			
			try {
				recCount = f.getRecCnt();
			} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
					| IOException e) {
				// TODO Auto-generated catch block
				status = FAIL;
				System.out.println("Can't get record count from the heap file.");
				e.printStackTrace();
			}
			if (status)
			{
				if (recCount != 10)
				{
					status = FAIL;
					System.out.println("Insert 10 records into the table, but only see " + recCount);
				}
			}
			else
			{
				System.out.println("Insert into query test fails");
				return status;
			}		
		}
		else
		{
			System.out.println("Insert into query test fails");
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
			System.out.println("Insert into query test fails");
			return status;
		}
		System.out.println("\nTesting the Insert Into query ...\nInsert into 5 records into the table \"sale_region\" created in test1.\n");
		for (int i = 0; i < 5; i ++)
		{
			System.out.println("Input Query is: " + insertQuery2[i]);
			int parseResult = Parser.parseQuery(insertQuery2[i]);
			ridList2[i] = Parser.rid;
			if (parseResult < 0)
			{
				System.out.println("Parsing the insert into query fails");
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
				System.out.println("No table named sale_region is found.");
				e.printStackTrace();
			}	
			
			try {
				recCount = f.getRecCnt();
			} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
					| IOException e) {
				// TODO Auto-generated catch block
				status = FAIL;
				System.out.println("Can't get record count from the heap file.");
				e.printStackTrace();
			}
			if (status)
			{
				if (recCount != 5)
				{
					status = FAIL;
					System.out.println("Insert 5 records into the table, but only see " + recCount);
				}
			}
			else
			{
				System.out.println("Insert into query test fails");
				return status;
			}		
		}
		else
		{
			System.out.println("Insert into query test fails");
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
			System.out.println("Insert into query test fails");
			return status;
		}
		if (status)
			System.out.println("Insert into query test is successful.");
		else
			System.out.println("Insert into query test fails");
		return status;
	}
	
	protected boolean test3 ()
	{
		System.out.println("\n-----------------------------------------------------Test3---------------------------------------------------------");
		System.out.println("\nTesting the Create Index query ...\n");
		boolean status = OK;
		
		String createIndexQuery1 = "create index id_index on sale_region ( sid ) integer;";
		String createIndexQuery2 = "create index name_index on sale_region ( sname ) string;";
		String createIndexQuery3 = "create index region_index on sale_region ( region ) rect;";
		
		System.out.println("\nTesting indexing on integer key ...");
		Parser.parseQuery(createIndexQuery1);
		
		BTreeFile indxFile = null;
		try {
			indxFile = new BTreeFile("id_index");
			
		} catch (GetFileEntryException | PinPageException | ConstructPageException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			e.printStackTrace();
		}
		BTFileScan scan = null;
		try {
			scan = indxFile.new_scan(null, null);
			
		} catch (KeyNotMatchException | IteratorException | ConstructPageException | PinPageException
				| UnpinPageException | IOException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			e.printStackTrace();
		}
		KeyDataEntry entry = null;
		try {
			entry = scan.get_next();
		} catch (ScanIteratorException e) {
			status = FAIL;
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int id = ((IntegerKey) (entry.key)).getKey();
		if (id != 101)
		{
			status = FAIL;
			System.out.println("The key sid value of the first record is not as expected. ");
		}
		System.out.println("\nTesting indexing on integer key is successful.");

		System.out.println("\nTesting indexing on string key ...");
		Parser.parseQuery(createIndexQuery2);
		try {
			indxFile = new BTreeFile("name_index");
			
		} catch (GetFileEntryException | PinPageException | ConstructPageException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			e.printStackTrace();
		}

		try {
			scan = indxFile.new_scan(null, null);
			
		} catch (KeyNotMatchException | IteratorException | ConstructPageException | PinPageException
				| UnpinPageException | IOException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			e.printStackTrace();
		}

		try {
			entry = scan.get_next();
		} catch (ScanIteratorException e) {
			status = FAIL;
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String sname = ((StringKey) (entry.key)).getKey();
		if (!sname.equals("Center"))
		{
			status = FAIL;
			System.out.println("The key sname value of the first record is not as expected. ");
		}
		System.out.println("\nTesting indexing on string key is successful.");
		
		System.out.println("\nTesting indexing on rect key ...");
		Parser.parseQuery(createIndexQuery3);
		try {
			indxFile = new BTreeFile("region_index");
			
		} catch (GetFileEntryException | PinPageException | ConstructPageException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			e.printStackTrace();
		}

		try {
			scan = indxFile.new_scan(null, null);
			
		} catch (KeyNotMatchException | IteratorException | ConstructPageException | PinPageException
				| UnpinPageException | IOException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			e.printStackTrace();
		}

		try {
			entry = scan.get_next();
		} catch (ScanIteratorException e) {
			status = FAIL;
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Rect region = ((RectKey) (entry.key)).getKey();
		if (!region.toString().equals("0.0,10.0,10.0,20.0"))
		{
			status = FAIL;
			System.out.println("The index key region value of the first record is not as expected. ");
		}
		System.out.println("\nTesting indexing on rect key is successful.");
		
		if (status)
		{
			System.out.println("\nCreate Index query is successful.");
		}
		
		return status;
	}
	
	protected boolean test4 ()
	{
		System.out.println("\n-----------------------------------------------------Test4---------------------------------------------------------");
		System.out.println("\nTesting the Select Area query ...\n");
		boolean status = OK;
		
		String selectAreaQuery1 = "select id, area ( shape ) from shopping_center";
		String selectAreaQuery2 = "select name, area ( shape ) from shopping_center";
		String selectAreaQuery3 = "select sname, area ( region ) from sale_region";
		System.out.println("Select Area query 1: " + selectAreaQuery1 + "\nThe results are:\n");
		Parser.parseQuery(selectAreaQuery1);
		System.out.println("Select Area query 1 finishes.\n");
		System.out.println("Select Area query 2: " + selectAreaQuery2 + "\nThe results are:\n");
		Parser.parseQuery(selectAreaQuery2);
		System.out.println("Select Area query 2 finishes.\n");
		System.out.println("Select Area query 3: " + selectAreaQuery3 + "\nThe results are:\n");
		Parser.parseQuery(selectAreaQuery3);
		System.out.println("Select Area query 3 finishes.\n");
		
		System.out.println("Select Area query test is successful.\n");
		return status;
	}
	
	protected boolean test5 ()
	{
		System.out.println("\n-----------------------------------------------------Test5---------------------------------------------------------");
		System.out.println("\nTesting the Select Intersection query ...\n");
		boolean status = OK;
		
		System.out.println("Query the intersection between the center sale region with all shopping centers");
		String selectIntersectionQuery[] = new String[10];
		for (int i = 0; i < 10 ; i ++)
		{
			selectIntersectionQuery[i] = "select intersection ( region, shape ) from sale_region, shopping_center where sid = 111 and id = " + (i + 1) + ";";
			Parser.parseQuery(selectIntersectionQuery[i]);
			Rect rect = Parser.intersection;
			String coordinates = "";
			if (rect != null)
			{
				coordinates = "[" + rect.toString() + "]";
				System.out.println("The coordinates of the intersection between the center sale region and shopping center id: " + (i  + 1) + " is: \n" + coordinates);
			}
			else
			{
				System.out.println("The center sale region has no intersection with shopping center id: " + (i + 1) + ".");
			}		
		}
		System.out.println("\nSelect Intersection query test is successful.");
		return status;
	}
	
	protected boolean test6 ()
	{
		System.out.println("\n-----------------------------------------------------Test6---------------------------------------------------------");
		System.out.println("\nTesting the Select Distance query ...\n");
		boolean status = OK;
		
		System.out.println("Query the intersection between the northeast sale region with all shopping centers");
		String selectDistanceQuery[] = new String[10];
		for (int i = 0; i < 10 ; i ++)
		{
			selectDistanceQuery[i] = "select distance ( region, shape ) from sale_region, shopping_center where sid = 102 and id = " + (i + 1) + ";";
			Parser.parseQuery(selectDistanceQuery[i]);
			Double distance = Parser.distance;
			System.out.println("The distance between the northeast sale region and shopping center id: " + (i  + 1) + " is: " + distance);			
		}
		System.out.println("\nSelect Distance query test is successful.");
		return status;
	}
}

public class SDOTest 
{
	public static void main (String[] args)
	{
		SDODriver sdoDriver = new SDODriver();
		boolean sdoStatus = sdoDriver.runTest();
		
		if (!sdoStatus)
		{
			 System.err.println ("Error encountered during " + sdoDriver.testName() + " tests:\n");
			 Runtime.getRuntime().exit(1);
		}
		
		Runtime.getRuntime().exit(0);
	}
}
