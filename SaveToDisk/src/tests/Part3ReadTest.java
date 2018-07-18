package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import sqlinterface.Parser;
import sqlinterface.TableInfo;

class Part3ReadDriver extends TestDriver implements GlobalConst
{
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	
	private String createTableQuery;
	private String insertQuery1[] = new String[5];
	private String insertQuery2[] = new String[10];
	private Heapfile catalogHF = null;	
	private Heapfile f = null;
	private String catalogName = "catalogTable";
	private String tableName = "";
	private ArrayList<String> attrNameList = new ArrayList<String>();
	private ArrayList<AttrType> attrTypeList = new ArrayList<AttrType>();
	

	public Part3ReadDriver ()
	{
		super("part3_");
		//dbpath = "/Users/conanxjp/Desktop/dbtests/"+"part3test.minibase-db";
	}
	
	protected String testName () 
	{
		return "Part 3 Read Test";
	}
	
	public boolean runTest()
	{
		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
	    
		    SystemDefs sysdef = new SystemDefs(dbpath,0,1000,"Clock");	// disk size set to 0 to open existing db file
		    
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
		
		    String tableName = "university_az";

		    String schema = "( uid integer primary key, uname string, ushape rect );";
		    
		    createTableQuery = "create table " + tableName + " " + schema;
		   
		    String saleRegionName[] = new String[5];
		    saleRegionName[0] = "North";
		    saleRegionName[1] = "South";
		    saleRegionName[2] = "West";
		    saleRegionName[3] = "East";
		    saleRegionName[4] = "All";
		    
		    int saleRegionId[] = new int[5];
		    saleRegionId[0] = 100;
		    saleRegionId[1] = 200;
		    saleRegionId[2] = 102;
		    saleRegionId[3] = 202;
		    saleRegionId[4] = 222;
		    
		    String salePerson[] = new String[5];
		    salePerson[0] = "Alice";
		    salePerson[1] = "Bob";
		    salePerson[2] = "Carol";
		    salePerson[3] = "David";
		    salePerson[4] = "Eston";
		    
		    String region[] = new String[5];
		    region[0] = "5, 10, 15, 20";
		    region[1] = "5, 0, 15, 10";
		    region[2] = "0, 5, 10, 15";
		    region[3] = "10, 5, 20, 15";
		    region[4] = "0, 0, 20, 20";
		    
		    for (int i = 0; i < 5; i ++)
		    {
			    insertQuery1[i] = "insert into sale_region ( sid, sname, region ) values ( " 
					    		+ saleRegionId[i] + ", " + saleRegionName[i] + ", ( " + region[i] + " ) );";
		    }
		    
		    String universityName[] = new String[10];
		    universityName[0] = "Arizona_State_University";
		    universityName[1] = "University_of_Arizona";
		    universityName[2] = "University_of_Phoenix";
		    universityName[3] = "Grand_Canyon_University";
		    universityName[4] = "Northern_Arizona_University";
		    universityName[5] = "Pima_Community_College";
		    universityName[6] = "Mesa_Community_College";
		    universityName[7] = "Chandler-Gilber_Community_College";
		    universityName[8] = "Rio_Salado_College";
		    universityName[9] = "Phoenix_College";
		    
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
			    insertQuery2[i] = "insert into university_az ( uid, uname, ushape ) values ( " + (idNum[i] + 1) + ", " + universityName[i] + ", ( " + rectValue + " ) );";
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
		System.out.println("\nTest read the db file from the disk ...\n");
		boolean status = OK;
		
		try {
			catalogHF = new Heapfile(catalogName);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("No table named " + catalogName + " is found.");
			e.printStackTrace();
			return status;
		}
		
		RID catalogRid = new RID();
		Tuple catalogTuple = new Tuple();
		
		Scan catalogScan = null;
		try {
			catalogScan = new Scan(catalogHF);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Loading catalog information fails");
			e.printStackTrace();
			return status;
		}
		
		try {
			catalogTuple = catalogScan.getNext(catalogRid);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Loading catalog information fails");
			e.printStackTrace();
			return status;
		}
		
		try {
			tableName = catalogTuple.getStrFld(1);
		} catch (FieldNumberOutOfBoundException | IOException e1) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Loading catalog information fails");
			e1.printStackTrace();
			return status;
		}
		
		while (catalogTuple != null) {
				
			try {
				String tempTableName= catalogTuple.getStrFld(1);
				if (tableName.equals(tempTableName)) {
					String attrName = catalogTuple.getStrFld(2);
					AttrType attrType = new AttrType(catalogTuple.getIntFld(3));
					attrNameList.add(attrName);
					attrTypeList.add(attrType);
				}
				else {
					ArrayList<String> nameList = new ArrayList<String>();
					nameList = attrNameList;
					attrNameList =  new ArrayList<String>();
					ArrayList<AttrType> typeList = new ArrayList<AttrType>();
					typeList = attrTypeList;
					attrTypeList = new ArrayList<AttrType>();
					
					TableInfo.addTable(tableName, nameList);
					TableInfo.addAttrName(nameList, typeList);
					//attrNameList.clear();
					//attrTypeList.clear();
					tableName = tempTableName;
					String attrName = catalogTuple.getStrFld(2);
					AttrType attrType = new AttrType(catalogTuple.getIntFld(3));
					attrNameList.add(attrName);
					attrTypeList.add(attrType);
				}
				catalogTuple = catalogScan.getNext(catalogRid);
			} catch (FieldNumberOutOfBoundException | IOException | InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				status = FAIL;
				System.err.println("Loading catalog information fails");
				e.printStackTrace();
				return status;
			} 
		}
		ArrayList<String> nameList = new ArrayList<String>();
		nameList = attrNameList;
		attrNameList =  new ArrayList<String>();
		ArrayList<AttrType> typeList = new ArrayList<AttrType>();
		typeList = attrTypeList;
		attrTypeList = new ArrayList<AttrType>();
		TableInfo.addTable(tableName, nameList);
		TableInfo.addAttrName(nameList, typeList);
	
		
		System.out.println("Loading catalog information successes\n");		
		System.out.println("Printing all records in table \"shopping_center\" ...");
		
		try {
			f = new Heapfile("shopping_center");
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Fails in loading the shopping_center table ...");
			e1.printStackTrace();
			return status;
		}
		Scan tableScan = null;
		try {
			tableScan = new Scan(f);
		} catch (InvalidTupleSizeException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for (int i = 0; i < 10; i ++)
		{			
			Tuple tuple;
			RID rid = new RID();
					
			try {
				tuple = tableScan.getNext(rid);
				ArrayList<String> attrNameList = TableInfo.tblInfo.get("shopping_center");
				int attrCnt = attrNameList.size();
				AttrType attrTypeList[] = new AttrType[attrCnt];
				for (int j = 0; j < attrCnt; j ++)
				{
					attrTypeList[j] = TableInfo.attrInfo.get(attrNameList.get(j));
				}
				tuple.print(attrTypeList);
			} catch (InvalidTupleSizeException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		}
		
		System.out.println("\nPrinting all records in table \"sale_region\" ...\n");
		
		try {
			f = new Heapfile("sale_region");
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Fails in loading the sale_region table ...");
			e1.printStackTrace();
			return status;
		}
		try {
			tableScan = new Scan(f);
		} catch (InvalidTupleSizeException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for (int i = 0; i < 5; i ++)
		{			
			Tuple tuple;
			RID rid = new RID();	
			
			try {
				tuple = tableScan.getNext(rid);
				ArrayList<String> attrNameList = TableInfo.tblInfo.get("sale_region");
				int attrCnt = attrNameList.size();
				AttrType attrTypeList[] = new AttrType[attrCnt];
				for (int j = 0; j < attrCnt; j ++)
				{
					attrTypeList[j] = TableInfo.attrInfo.get(attrNameList.get(j));
				}
				tuple.print(attrTypeList);
			} catch (InvalidTupleSizeException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		}
		
		System.out.println("\nCreate a new table \"university_az\" ...");
		Parser.parseQuery(createTableQuery);
		
		System.out.println("\nInsert some records into \"university_az\" ...");
		for (int i = 0; i < 10; i ++)
		{
			Parser.parseQuery(insertQuery2[i]);
		}
		
		try {
			f = new Heapfile("university_az");
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Fails in loading the university_az table ...");
			e1.printStackTrace();
			return status;
		}
		try {
			tableScan = new Scan(f);
		} catch (InvalidTupleSizeException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println("\nPrinting all records in table \"university_az\" after insertion ...");
		for (int i = 0; i < 10; i ++)
		{			
			Tuple tuple;
			RID rid = new RID();	
			
			try {
				tuple = tableScan.getNext(rid);
				ArrayList<String> attrNameList = TableInfo.tblInfo.get("university_az");
				int attrCnt = attrNameList.size();
				AttrType attrTypeList[] = new AttrType[attrCnt];
				for (int j = 0; j < attrCnt; j ++)
				{
					attrTypeList[j] = TableInfo.attrInfo.get(attrNameList.get(j));
				}
				tuple.print(attrTypeList);
			} catch (InvalidTupleSizeException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		}
		
		System.out.println("\nInsert some records into \"sale_region\" ...");
		for (int i = 0; i < 5; i ++)
		{
			Parser.parseQuery(insertQuery1[i]);
		}
		
		try {
			f = new Heapfile("sale_region");
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			// TODO Auto-generated catch block
			status = FAIL;
			System.err.println("Fails in loading the sale_region table ...");
			e1.printStackTrace();
			return status;
		}
		try {
			tableScan = new Scan(f);
		} catch (InvalidTupleSizeException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println("\nPrinting all records in table \"sale_region\" after insertion ...\n");
		for (int i = 0; i < 10; i ++)
		{			
			Tuple tuple;
			RID rid = new RID();	
			
			try {
				tuple = tableScan.getNext(rid);
				ArrayList<String> attrNameList = TableInfo.tblInfo.get("sale_region");
				int attrCnt = attrNameList.size();
				AttrType attrTypeList[] = new AttrType[attrCnt];
				for (int j = 0; j < attrCnt; j ++)
				{
					attrTypeList[j] = TableInfo.attrInfo.get(attrNameList.get(j));
				}
				tuple.print(attrTypeList);
			} catch (InvalidTupleSizeException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
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

public class Part3ReadTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Part3ReadDriver part3ReadDriver = new Part3ReadDriver();
		boolean part3Status = part3ReadDriver.runTest();
		if (!part3Status)
		{
			System.err.println ("Error encountered during " + part3ReadDriver.testName() + " tests:\n");
			 Runtime.getRuntime().exit(1);
		}
		
		Runtime.getRuntime().exit(0);
	}

}
