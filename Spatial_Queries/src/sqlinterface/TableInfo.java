// by RUOPENG LIU
// CSE510 

package sqlinterface;

import java.util.*;
import global.*;

public class TableInfo {
	
	//the first element in tblAttribute is key attribute
	
	//map tablename with attribute list
	public static HashMap<String,ArrayList<String>> tblInfo = new HashMap<String, ArrayList<String>>();
	
	//map attribute name with attribute type one on one
	public static HashMap<String,AttrType> attrInfo = new HashMap<String, AttrType>();
	
	//add table name and table attribute list to table info hashMap
	public static void addTable(String tableName, ArrayList<String> attrNameList){
		if(tblInfo.containsKey(tableName)){
			System.out.printf("Error,Table %s already existed", tableName);
			return ;
		}
		else
		{
			//put the table name into the hashmap
			tblInfo.put(tableName, attrNameList);
		}
	}
	
	public static void addAttrName(ArrayList<String> attrNameList, ArrayList<AttrType> attrTypeList){
		String attName;
		int listLen = attrNameList.size();
		for (int index = 0; index < listLen; index++){
			attName = attrNameList.get(index);
			
			//here I suppose that each attribute name has only one attribute type
			if(!attrInfo.containsKey(attName)){
				//if attribute name not in the list, add it into the list.
				attrInfo.put(attName, attrTypeList.get(index));
			}
				
		}
			
	}
	
	
	


}
