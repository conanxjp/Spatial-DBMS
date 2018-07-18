package sqlinterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import global.*;
import heap.*;
import btree.*;

/*
 * CREATE INDEX idxname ON tablename ( colname ) idxtype;
 * 
 */
public class CreateIndex {

	public static void createIndex(String indexName, String tableName, String indexKeyName, AttrType indexKeyType)
	{
		//System.out.println("Index file name = " + indexName + "\n" + "Table file name = " + tableName + "\n" + "Index key name = " + indexKeyName +"\n" + "Index key type = " + indexKeyType);
		
		// check if tablename exist in TableInfo
		if (!TableInfo.tblInfo.containsKey(tableName)) {
			System.err.println("Table has not been created!");
			return;
		}

		// check if indexKeyName and indexKeyType match TableInfo
		if (TableInfo.attrInfo.get(indexKeyName).toString() != indexKeyType.toString()) {
			System.out.println(TableInfo.attrInfo.get(indexKeyName).toString());
			System.out.println(indexKeyType.toString());
			System.err.println("The name and type of key field do not match!");
			return;
		}
		
		
		//get key field number
		int keyFieldNo = 0;
		ArrayList<String> field = TableInfo.tblInfo.get(tableName);
		keyFieldNo = field.indexOf(indexKeyName) + 1; //keyFieldNo starts with 1 not 0
		//System.out.println("key field number = " + keyFieldNo);
		
		 
		//create a BTreeFile class, if exist, it will open, otherwise it will create a new one
		BTreeFile indexFile = null;
		try {
			indexFile = new BTreeFile(indexName, indexKeyType.attrType, 32, DeleteFashion.FULL_DELETE); // max key size: 32 byte; 
		} catch (GetFileEntryException | ConstructPageException | AddFileEntryException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		} 
		
		
		// find table heapfile based on tableName
		Heapfile hpFile = null;
		try {
			hpFile= new Heapfile(tableName);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		} 

		
		// 
		// create a scan of the heapfile
		Scan hpFileScan = null;
		try {
			//hpFileScan = new Scan(hpFile);
			hpFileScan = hpFile.openScan();
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		} 

		
		// get the first record of the table (heapfile)
		RID rid = new RID();
		Tuple record = new Tuple();
		
		//System.out.println(rid.pageNo + ";" + rid.slotNo);

		try {
			record = hpFileScan.getNext(rid);		

//			System.out.println("Record field count = " + record.getFldCnt());
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		} 

		
		// iterate to get all records of the table (heapfile) and insert <indexKey, rid> to B-tree index
		while (record != null){
			

						
			try {

				//System.out.println("rid = " + rid.pageNo + ";" + rid.slotNo);
				
				switch(indexKeyType.attrType){
				case AttrType.attrInteger:
					int key1 = record.getIntFld(keyFieldNo);
					//System.out.println("key1 = " + key1);
					indexFile.insert(new IntegerKey(key1),rid);
					break;
				case AttrType.attrString:
					String key2 = record.getStrFld(keyFieldNo);
					indexFile.insert(new StringKey(key2),rid);
					break;
				case AttrType.attrRect:
					Rect key3 = record.getRectFld(keyFieldNo);
					indexFile.insert(new RectKey(key3),rid);
					break;
				}
								
				record = hpFileScan.getNext(rid);
			}
			catch (InvalidTupleSizeException | IOException | KeyTooLongException | KeyNotMatchException | LeafInsertRecException | IndexInsertRecException | ConstructPageException | UnpinPageException | PinPageException | NodeNotMatchException | ConvertException | DeleteRecException | IndexSearchException | IteratorException | LeafDeleteException | InsertException | FieldNumberOutOfBoundException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
			
			
		}
		
		// close scan
		hpFileScan.closescan();
		//System.out.println("Index has been created successfully!");
		
	}
}
