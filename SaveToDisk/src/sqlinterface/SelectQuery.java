package sqlinterface;

import java.io.IOException;
import java.util.ArrayList;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.Operand;
import iterator.RelSpec;
import iterator.TupleUtilsException;

public class SelectQuery {

	public static void selectArea (String tableName, ArrayList<String> attrNameList, ArrayList<String> rectNameList)
	{
		ArrayList<String> fldList = TableInfo.tblInfo.get(tableName);
		int selectFldCnt = attrNameList.size();
		ArrayList<AttrType> typeList = new ArrayList<AttrType>();
		if (selectFldCnt > 0)
		{
			for (int i = 0 ; i < selectFldCnt; i ++)
			{
				typeList.add(TableInfo.attrInfo.get(attrNameList.get(i)));
			}
		}
		
		Heapfile hpFile = null;
		try {
			hpFile= new Heapfile(tableName);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		Scan hpFileScan = null;
		try {
			hpFileScan = new Scan(hpFile);
			//hpFileScan = hpFile.openScan();
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
			e.printStackTrace();
		} 
		RID rid = new RID();
		Tuple fromTuple = new Tuple();
		//Tuple toTuple = new Tuple();
		
		/*
		 * Here the tempNamesfx may be too long, because the maxmium length of filename is 50
		 * MAX_NAME in global when we have more field, the lengh may exceed 50 and cause error.
		 * 
		 */
		String tempNameSfx = "";
		for (int i = 0; i < attrNameList.size(); i ++)
		{
			tempNameSfx += attrNameList.get(i) + "_";
		}
		tempNameSfx = tempNameSfx.substring(0, tempNameSfx.length() - 1);
		String selectTableName = "selectarea_" + tableName + tempNameSfx;
		ArrayList<String> selectNameList = new ArrayList<String>();
		for (int i = 0; i < selectFldCnt; i ++)
		{
			selectNameList.add(attrNameList.get(i));
		}
		selectNameList.add("area");
		typeList.add(new AttrType(AttrType.attrReal));
		TableInfo.addTable(selectTableName, selectNameList);
		TableInfo.addAttrName(selectNameList, typeList);
		Heapfile selecthpFile = null;
		
		try {
			selecthpFile = new Heapfile(selectTableName);
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		InsertInto insertObj = new InsertInto();
		
		try {
			fromTuple = hpFileScan.getNext(rid);
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (fromTuple != null)
		{
			ArrayList<String> valueList = new ArrayList<String>();
			try {		
				for (int i = 0; i < selectFldCnt; i ++)
				{
					int fldNo = SelectQuery.getFldNo(attrNameList.get(i), fldList);
					AttrType type = TableInfo.attrInfo.get(attrNameList.get(i));
					switch (type.attrType)
					{
						case AttrType.attrInteger:
							try {
								valueList.add(Integer.toString(fromTuple.getIntFld(fldNo)));
							} catch (FieldNumberOutOfBoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							break;
						case AttrType.attrReal:
							try {
								valueList.add(Float.toString(fromTuple.getFloFld(fldNo)));
							} catch (FieldNumberOutOfBoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							break;
						case AttrType.attrString:
							try {
								valueList.add(fromTuple.getStrFld(fldNo));
							} catch (FieldNumberOutOfBoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							break;
						case AttrType.attrRect:
							try {
								valueList.add(fromTuple.getRectFld(fldNo).toString());
							} catch (FieldNumberOutOfBoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							break;		
					}			
				}
				try {
					String area = Double.toString(fromTuple.getRectFld(SelectQuery.getFldNo(rectNameList.get(0), fldList)).area());
					valueList.add(area);
				} catch (FieldNumberOutOfBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				insertObj.insertInto(selectTableName, selectNameList, valueList);
				fromTuple = hpFileScan.getNext(rid);	
			} catch (InvalidTupleSizeException | IOException e) {
				// TODO Auto-generated catch block
				System.err.println(e.getMessage());
				e.printStackTrace();
			} 
		}
		
		Scan selectScan = null;
		try {
			selectScan = new Scan(selecthpFile);
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String selectHdr  = "";
		System.out.print("[");
		for (int i = 0; i < selectNameList.size(); i ++)
		{
			selectHdr += selectNameList.get(i) + ", ";
		}
		selectHdr = selectHdr.substring(0, selectHdr.length() - 2);
		System.out.println(selectHdr + "]");
		try {
			fromTuple = selectScan.getNext(rid);
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (fromTuple != null)
		{
			try {
				AttrType sType[] = new AttrType[typeList.size()];
				for (int i = 0; i < typeList.size(); i ++)
				{
					sType[i] = typeList.get(i);
				}
				fromTuple.print(sType);
				fromTuple = selectScan.getNext(rid);
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}
	
	private static int getFldNo(String attrName, ArrayList<String> attrNameList)
	{
		for (int i = 0; i < attrNameList.size(); i ++)
		{
			if (attrName.equals(attrNameList.get(i)))
				return i + 1;
		}
		return -1;
	}
	
	private static void setCondExprOpSymbol2 (Operand op, AttrType type, String value)
	{
		switch (type.attrType)
		{
			case AttrType.attrInteger:
				op.integer = Integer.parseInt(value);
				break;
			case AttrType.attrReal:
				op.real = Float.parseFloat(value);
				break;
			case AttrType.attrString:
				op.string = value;
				break;
		}
	}
	
	public static Rect selectIntersection (String tableName1, String tableName2, ArrayList<String> rectNameList, ArrayList<String> attrNameList, ArrayList<AttrOperator> attrOpList, ArrayList<String> attrValueList)
	{
		ArrayList<String> fldList1 = TableInfo.tblInfo.get(tableName1);
		ArrayList<String> fldList2 = TableInfo.tblInfo.get(tableName2);	
		AttrType type1 = TableInfo.attrInfo.get(attrNameList.get(0));
		AttrType type2 = TableInfo.attrInfo.get(attrNameList.get(1));
		Rect rect1 = null;
		Rect rect2 = null;
		/*
		AttrType typeList1[] = new AttrType[fldList1.size()];
		AttrType typeList2[] = new AttrType[fldList2.size()];
		short strSize1[] = new short[1];
		strSize1[0] = 30;
		short strSize2[] = new short[1];
		strSize2[0] = 30;
		
		for (int i = 0; i < fldList1.size(); i ++)
		{
			typeList1[i] = TableInfo.attrInfo.get(fldList1.get(i));
		}
		for (int i = 0; i < fldList2.size(); i ++)
		{
			typeList2[i] = TableInfo.attrInfo.get(fldList2.get(i));
		}
		
		CondExpr conditions[] = new CondExpr[3];
		conditions[0] = new CondExpr();
		conditions[1] = new CondExpr();
		conditions[2] = new CondExpr();
		conditions[0].next = null;
		conditions[0].op = attrOpList.get(0);
		conditions[0].type1 = new AttrType(AttrType.attrSymbol);
		conditions[0].type2 = TableInfo.attrInfo.get(attrNameList.get(0));
		conditions[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), SelectQuery.getFldNo(attrNameList.get(0), fldList1));
		SelectQuery.setCondExprOpSymbol2(conditions[0].operand2, type1, attrValueList.get(0));
		
		conditions[1].next = null;
		conditions[1].op = attrOpList.get(1);
		conditions[1].type1 = new AttrType(AttrType.attrSymbol);
		conditions[1].type2 = TableInfo.attrInfo.get(attrNameList.get(1));
		conditions[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), SelectQuery.getFldNo(attrNameList.get(1), fldList2));
		SelectQuery.setCondExprOpSymbol2(conditions[0].operand2, type2, attrValueList.get(1));
		
		conditions[2] = null;
		
		FldSpec projection1[] = new FldSpec[fldList1.size()];
		for (int i = 0; i < fldList1.size(); i ++)
		{
			projection1[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		FldSpec projection2[] = new FldSpec[fldList2.size()];
		for (int i = 0; i < fldList2.size(); i ++)
		{
			projection2[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		
		FileScan fileScan1 = null;	
		try {
			fileScan1 = new FileScan(tableName1, typeList1, strSize1,
						  (short)fldList1.size(), (short)fldList1.size(),
						  projection1, null);
		} catch (FileScanException | TupleUtilsException | InvalidRelation | IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		FileScan fileScan2 = null;	
		try {
			fileScan2 = new FileScan(tableName2, typeList2, strSize2,
						  (short)fldList2.size(), (short)fldList2.size(),
						  projection2, null);
		} catch (FileScanException | TupleUtilsException | InvalidRelation | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		Heapfile hpFile1 = null;
		try {
			hpFile1 = new Heapfile(tableName1);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Heapfile hpFile2 = null;
		try {
			hpFile2 = new Heapfile(tableName2);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scan scan1 =  null;
		try {
			scan1 = new Scan(hpFile1);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scan scan2 =  null;
		try {
			scan2 = new Scan(hpFile2);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		RID rid1 = new RID();
		RID rid2 = new RID();
		Tuple tuple1 = new Tuple();
		Tuple tuple2 = new Tuple();
		
		try {
			tuple1 = scan1.getNext(rid1);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (tuple1 != null)
		{
			int fldNo = SelectQuery.getFldNo(attrNameList.get(0), fldList1);
			String fldValue = "";
			switch (type1.attrType)
			{
				case AttrType.attrInteger:
					try {
						fldValue = Integer.toString(tuple1.getIntFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrReal:
					try {
						fldValue = Float.toString(tuple1.getFloFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrString:
					try {
						fldValue = tuple1.getStrFld(fldNo);
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
			}
			if (fldValue.equals(attrValueList.get(0)))
			{
				try {
					rect1 = tuple1.getRectFld(SelectQuery.getFldNo(rectNameList.get(0), fldList1));
				} catch (FieldNumberOutOfBoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				tuple1 = scan1.getNext(rid1);
			} catch (InvalidTupleSizeException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			tuple2 = scan2.getNext(rid2);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (tuple2 != null)
		{
			int fldNo = SelectQuery.getFldNo(attrNameList.get(1), fldList2);
			String fldValue = "";
			switch (type2.attrType)
			{
				case AttrType.attrInteger:
					try {
						fldValue = Integer.toString(tuple2.getIntFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrReal:
					try {
						fldValue = Float.toString(tuple2.getFloFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrString:
					try {
						fldValue = tuple2.getStrFld(fldNo);
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
			}
			if (fldValue.equals(attrValueList.get(1)))
			{
				try {
					rect2 = tuple2.getRectFld(SelectQuery.getFldNo(rectNameList.get(1), fldList2));
				} catch (FieldNumberOutOfBoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				tuple2 = scan2.getNext(rid2);
			} catch (InvalidTupleSizeException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		Rect rectIntersect = rect1.intersect(rect2);
		//System.out.println(rectIntersect.toString());
		return rectIntersect;
	}
	
	public static double selectDistance (String tableName1, String tableName2, ArrayList<String> rectNameList, ArrayList<String> attrNameList, ArrayList<AttrOperator> attrOpList, ArrayList<String> attrValueList)
	{
		ArrayList<String> fldList1 = TableInfo.tblInfo.get(tableName1);
		ArrayList<String> fldList2 = TableInfo.tblInfo.get(tableName2);	
		AttrType type1 = TableInfo.attrInfo.get(attrNameList.get(0));
		AttrType type2 = TableInfo.attrInfo.get(attrNameList.get(1));
		Rect rect1 = null;
		Rect rect2 = null;
		
		Heapfile hpFile1 = null;
		try {
			hpFile1 = new Heapfile(tableName1);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Heapfile hpFile2 = null;
		try {
			hpFile2 = new Heapfile(tableName2);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scan scan1 =  null;
		try {
			scan1 = new Scan(hpFile1);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scan scan2 =  null;
		try {
			scan2 = new Scan(hpFile2);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		RID rid1 = new RID();
		RID rid2 = new RID();
		Tuple tuple1 = new Tuple();
		Tuple tuple2 = new Tuple();
		
		try {
			tuple1 = scan1.getNext(rid1);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (tuple1 != null)
		{
			int fldNo = SelectQuery.getFldNo(attrNameList.get(0), fldList1);
			String fldValue = "";
			switch (type1.attrType)
			{
				case AttrType.attrInteger:
					try {
						fldValue = Integer.toString(tuple1.getIntFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrReal:
					try {
						fldValue = Float.toString(tuple1.getFloFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrString:
					try {
						fldValue = tuple1.getStrFld(fldNo);
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
			}
			if (fldValue.equals(attrValueList.get(0)))
			{
				try {
					rect1 = tuple1.getRectFld(SelectQuery.getFldNo(rectNameList.get(0), fldList1));
				} catch (FieldNumberOutOfBoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				tuple1 = scan1.getNext(rid1);
			} catch (InvalidTupleSizeException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			tuple2 = scan2.getNext(rid2);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (tuple2 != null)
		{
			int fldNo = SelectQuery.getFldNo(attrNameList.get(1), fldList2);
			String fldValue = "";
			switch (type2.attrType)
			{
				case AttrType.attrInteger:
					try {
						fldValue = Integer.toString(tuple2.getIntFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrReal:
					try {
						fldValue = Float.toString(tuple2.getFloFld(fldNo));
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case AttrType.attrString:
					try {
						fldValue = tuple2.getStrFld(fldNo);
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
			}
			if (fldValue.equals(attrValueList.get(1)))
			{
				try {
					rect2 = tuple2.getRectFld(SelectQuery.getFldNo(rectNameList.get(1), fldList2));
				} catch (FieldNumberOutOfBoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				tuple2 = scan2.getNext(rid2);
			} catch (InvalidTupleSizeException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		double distance = rect1.distance(rect2);
		//System.out.println(distance);
		return distance;
	}		
}
