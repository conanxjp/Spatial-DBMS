package tests;

import static org.junit.Assert.*;

import org.junit.Test;

import btree.*;
import global.*;

import java.io.IOException;
import java.util.*;

import heap.*;
import sqlinterface.*;


public class CreateIndexTest extends TestDriver {

	@Test
	public void testCreateIndex() {
		dbpath = "/tmp/CreatIndex.db"; 
		SystemDefs sysdef = new SystemDefs(dbpath,1000,100,"Clock");
		
		
		String fileName = "student";
		
		ArrayList<String> attrList = new ArrayList<String>();
		ArrayList<AttrType> typeList = new ArrayList<AttrType>();
		attrList.add("name");
		typeList.add(new AttrType(AttrType.attrString));
		attrList.add("id");
		typeList.add(new AttrType(AttrType.attrInteger));
		
		
		Heapfile f = CreateTable.createTable(fileName, attrList, typeList);
		
		ArrayList<String> valueList = new ArrayList<String>();
		valueList.add("Adam");
		valueList.add("1000");
		InsertInto cmd = new InsertInto();
		try {
			cmd.insertInto(fileName, attrList, valueList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		valueList.clear();
		valueList.add("Bob");
		valueList.add("2000");
		try {
			cmd.insertInto(fileName, attrList, valueList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
/*			
		// test code for scan and tuple operation
		try {
			//Heapfile f = new Heapfile("student");
			System.out.println("Total Record: " + f.getRecCnt());
			
			//test to extract record
			Scan scan = f.openScan();
			RID rid = new RID();
			Tuple record = new Tuple();
			
			//test code
			record = scan.getNext(rid);
			System.out.println("record length = " + record.getLength());
			for (byte bt : record.returnTupleByteArray()) {
				System.out.printf("record byte = 0x%02X \n", bt);
			}
			
			System.out.println("record byte = " + Arrays.toString(record.returnTupleByteArray()));
			
			System.out.println("Filed Count = " + record.getFldCnt());
			
			System.out.println("RID = " + rid.pageNo + rid.slotNo);
			
			//test B+ tree index 
			BTreeFile btFile = new BTreeFile("BindexFile",AttrType.attrInteger,32,DeleteFashion.FULL_DELETE);
			btFile.insert(new IntegerKey(record.getIntFld(2)), rid);
			
			try {
				System.out.println("First string = " + record.getStrFld(1));
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (InvalidTupleSizeException | HFBufMgrException | HFDiskMgrException | IOException | InvalidSlotNumberException | GetFileEntryException | ConstructPageException | AddFileEntryException | KeyTooLongException | KeyNotMatchException | LeafInsertRecException | IndexInsertRecException | UnpinPageException | PinPageException | NodeNotMatchException | ConvertException | DeleteRecException | IndexSearchException | IteratorException | LeafDeleteException | InsertException | FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
*/		
		
		CreateIndex.createIndex("testIndex", "student", "id", new AttrType(AttrType.attrInteger));
		
		try{
			//Heapfile f = new Heapfile("student"); //open the existing heapfile
			BTreeFile indxFile = new BTreeFile("testIndex");
			
			//scan all in index file
			BTFileScan scan = indxFile.new_scan(null, null);
			KeyDataEntry entry = scan.get_next();
			int id = ((IntegerKey) (entry.key)).getKey();
			assertEquals(id,1000);
			RID rid = ((LeafData) entry.data).getData();
			assertEquals("key does not match the info retrieved from RID", id, f.getRecord(rid).getIntFld(2));
			
			entry = scan.get_next();
			id = ((IntegerKey) (entry.key)).getKey();
			assertEquals(id,2000);
			rid = ((LeafData) entry.data).getData();
			assertEquals("key does not match the info retrieved from RID", id, f.getRecord(rid).getIntFld(2));
			
		} catch(Exception e) {
			e.printStackTrace();
		}

		
		
		//fail("Not yet implemented");
	}

}
