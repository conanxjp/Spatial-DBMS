package sqlinterface;

import java.io.IOException;
import java.util.ArrayList;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;

public class CreateTable {
	
	public static Heapfile createTable(String tableName, ArrayList<String> attrNameList, ArrayList<AttrType> attrTypeList)	{
		TableInfo.addTable(tableName, attrNameList);
		TableInfo.addAttrName(attrNameList, attrTypeList);
		
		InsertInto insertObj = new InsertInto();

		
	    ArrayList<String> catalogIndexFieldList = TableInfo.tblInfo.get("catalogTable");
		
		int arrLen = attrNameList.size();
				
		ArrayList<String> catalogAttrValueList = new ArrayList<String>();
		//String insertQuery = "insert into cti ( TN, FN, ITFT ) values ( " + tableName + ", " + attrNameList.get(0) +", " + Integer.toString(attrTypeList.get(0).attrType) + " );";
		
		for (int index = 0; index < arrLen; index++){
			catalogAttrValueList.clear();
			catalogAttrValueList.add(tableName);		//table file name
			catalogAttrValueList.add(attrNameList.get(index));	
			catalogAttrValueList.add(Integer.toString(attrTypeList.get(index).attrType));
			
			//insert one record to CatalogTableIndex table		
			try {
				insertObj.insertInto("catalogTable", catalogIndexFieldList, catalogAttrValueList);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		
		}
		
		//Parser.parseQuery(insertQuery);
		
				
		
		//create an index record tuple and insert the record into the index heap file
		
		
		Heapfile f = null;
		try {
			//System.out.println(tableName);
			f = new Heapfile(tableName);
			} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println(f.toString());
		
	
			 return f;
			 
	}	 
		 
	
}
