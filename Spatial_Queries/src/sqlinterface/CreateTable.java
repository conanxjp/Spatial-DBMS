package sqlinterface;

import java.io.IOException;
import java.util.ArrayList;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;

public class CreateTable {
	
	public static Heapfile createTable(String tableName, ArrayList<String> attrNameList, ArrayList<AttrType> attrTypeList)	{
		TableInfo.addTable(tableName, attrNameList);
		TableInfo.addAttrName(attrNameList, attrTypeList);
		
		
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
