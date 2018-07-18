package sqlinterface;

import java.io.IOException;
import java.util.ArrayList;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import chainexception.ChainException;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;




public class InsertInto {

	public static boolean DEBUG = false;
	
	RID rid = new RID();
	public void insertInto(String tableName, ArrayList<String> attrNameList, ArrayList<String> attrValueList) throws IOException
	{
		// try to search the in the table name hash map, to see whether the table already exists.
		//if not exists, print error and return.
		if(!TableInfo.tblInfo.containsKey(tableName)){
			System.out.printf("Table " + tableName + " does not exist--by Group");
			return;
		}
		
		//open the table with a name of "tableName"
		Heapfile f = null;
		try {
			//System.out.println(tableName);
			f = new Heapfile(tableName);
			} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println(f.toString());
		
		//check whether the attribute list match the table
		Tuple t = new Tuple();
		int fldNum = attrNameList.size();	//number of field
		AttrType mtype[] = new AttrType[fldNum];
		int stringcount = 0;
		for (int i = 0; i < fldNum; i ++)
		{
			mtype[i] = TableInfo.attrInfo.get(attrNameList.get(i));
			if (mtype[i].attrType  == AttrType.attrString)
			{
				stringcount ++;
			}
		}
		short Msizes[] = new short[stringcount];
		int stringIndex = 0;
		//get the size of Strings in the tuple and store into Msizes
		for (int i = 0; i < fldNum; i ++)
		{
	
			//mtype[i] = TableInfo.attrInfo.get(attrNameList.get(i));
			if (mtype[i].attrType  == AttrType.attrString)
			{
				Msizes[stringIndex] = (short)attrValueList.get(i).length();
				stringIndex ++;
			}
		}
		
		
		try {
			t.setHdr((short) fldNum, mtype, Msizes);
		} catch (InvalidTypeException | InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//get the value of attributes
		//int attCount = attrNameList.size();
		//int tupleLen = 0;
/*		no longer need to get length of record
 * 		
 *
		//traverse the attribute to get the length of record
		for(int index = 0; index < attCount; index ++){
			String tmpName = attrNameList.get(index);
			AttrType tmpType = TableInfo.attrInfo.get(tmpName);
			
			switch (tmpType.attrType){
			case 0: //attrString: //= 0
			//length of string
				tupleLen += attrValueList.get(index).length();
				break;
		
			case 1: //attrInteger: //= 1;
				tupleLen += 4;
				break;
			case 2: //attrReal :   //= 2;
				tupleLen +=8;
				break;
			
	//		case: attrSymbol  //= 3;
			case 4: //attrRect:    //= 4;
				tupleLen += 32;
				break;
			
			}
		}
		
		//now I get the length of tuple, create a new tuple and insert
		
		
		byte[] data = new byte[tupleLen];
*/
		int size = t.size();
		t = new Tuple(size);
		try {
			t.setHdr((short) fldNum, mtype, Msizes);
		} catch (InvalidTypeException | InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if(DEBUG){
			System.out.printf("The tuple field count is %d\n", t.getFldCnt());
		}
		
		//write to data, which is array of Byte
		for(int fldNo= 1; fldNo <= fldNum; fldNo ++){
			String tmpName = attrNameList.get(fldNo - 1);
			//use hash map fuction to find the attrType with the name of attribute
			AttrType tmpType = TableInfo.attrInfo.get(tmpName);
		//	int pos = 0;
			switch (tmpType.attrType){
			case AttrType.attrString : //0: 
			//add string value to the tuple
				try {
					t.setStrFld(fldNo, attrValueList.get(fldNo-1));
					
				} catch (FieldNumberOutOfBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//Convert.setStrValue(attrValueList.get(index), pos, data);
				//pos += attrValueList.get(index).length();
				if(DEBUG){
					try {
						System.out.printf("The string attrbute is %s\n", t.getStrFld(fldNo));
					} catch (FieldNumberOutOfBoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				break;
		
			case AttrType.attrInteger: //1: 
				try {
					t.setIntFld(fldNo, Integer.valueOf(attrValueList.get(fldNo - 1)));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FieldNumberOutOfBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//Convert.setIntValue(Integer.valueOf(attrValueList.get(index)), pos, data);
		//		pos +=4;
				break;
			case AttrType.attrReal : //attrReal :   //= 2;
				try {
					t.setFloFld(fldNo, Float.parseFloat(attrValueList.get(fldNo - 1)));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FieldNumberOutOfBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//Convert.setFloValue(Float.parseFloat(attrValueList.get(index)), pos, data);
				//pos += 4;
				break;
			
	//		case: Attrtype.attrSymbol  //= 3;
			case AttrType.attrRect: //attrRect:    //= 4;
				String[] output = attrValueList.get(fldNo -1).split(",");
			    double x1 = Double.parseDouble(output[0]); 
			    double y1 = Double.parseDouble(output[1]); 
			    double x2 = Double.parseDouble(output[2]); 
			    double y2 = Double.parseDouble(output[3]); 
			    Rect rect = new Rect(x1,y1,x2,y2);
				try {
					t.setRectFld(fldNo, rect);
				} catch (FieldNumberOutOfBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			   // Convert.setRectValue(rect, pos, data);
				//pos += 32;
				break;
			
			}
		
		}
		//insert record
		try {
			  //rid = f.insertRecord(data);
			//insert the who tuple byte array, header then tuple field value array
			rid = f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
			  
			  e.printStackTrace();
			}
	
	}
}
