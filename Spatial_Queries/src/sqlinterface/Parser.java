package sqlinterface;

import java.io.IOException;
import java.lang.Object;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import global.*;

/*
 * Query Syntax
 * CREATE TABLE tablename (col1 type1, col2 type2,...);
 * CREATE INDEX idxname ON tablename ( colname ) idxtype;
 * INSERT INTO tablename ( col1, col2, ... ) VALUES ( val1, val2, ... );
 * SELECT col1, col2, ..., AREA ( shapename ) FROM tablename
 * SELECT INTERSECTION ( shape1, shape2 ) FROM table1, table2 WHERE col1 = val1 AND col2 = val2;
 * DISTANCE is the same as AREA
 */


public class Parser {
	
	private static String tableName = null; // table name
	private static String tableName2 = null; // 2nd table name for select query
	private static String indexName = null;; // index name
	private static String indexKeyName = null;; // index key name
	private static AttrType indexKeyType; // index key type
	private static ArrayList<String> attrNameList = new ArrayList<String>();	// attribute name list
	private static ArrayList<AttrType> attrTypeList = new ArrayList<AttrType>();	// attribute type list
	private static ArrayList<String> attrValueList = new ArrayList<String>(); // attribute value list
	private static ArrayList<String> rectNameList = new ArrayList<String>();	// attribute name list
	private static ArrayList<AttrOperator> attrOpList = new ArrayList<AttrOperator>(); // attribute operator list
	private static boolean queryType[] = new boolean[6];
	public static RID rid = new RID();
	public static Rect intersection = null;
	public static Double distance = null;
	
	public static int parseQuery(String query)
	{
		attrNameList = new ArrayList<String>();
		attrTypeList = new ArrayList<AttrType>();
		attrValueList = new ArrayList<String>();
		rectNameList = new ArrayList<String>();
		tableName = new String("");
		tableName2 = new String("");
		rid = new RID();
		intersection = null;
		distance = null;
		
		Parser.queryTypeInit();
		
		if (query.equals("") || query.matches("//s+"))
		{
			System.out.println("ERROR! Query is empty!");
			return -1;
		}
		String parsedList1[] = query.split(" ");
		
		int index = 0;
		String tok = parsedList1[index++];
	
		if (tok.toLowerCase().equals("create"))
		{
			tok = parsedList1[index++];
		
			if (tok.toLowerCase().equals("table"))
			{
				queryType[0] = true;
				if (parsedList1.length < 5)
				{
					System.out.println("ERROR! CREATE TABLE Query is missing key fields!");
					return -1;
				}
				tableName = parsedList1[index++];
				if (tableName.matches("^\\w.*$"))
				{
					if (tableName.matches(".*\\W+.*"))
					{
						System.out.println("ERROR! Table Name \"" + tableName + "\" contains non-alphanumeric characters");
						return -1;
					}
					else
					{
						tok = parsedList1[index];
						if (tok.startsWith("("))
						{
							if (tok.equals("("))
							{
								index ++;
								tok = parsedList1[index++];
							}
							else
							{
								index++;
								tok = tok.substring(1, tok.length());
							}
							if (tok.matches(".*\\W+.*"))
							{
								System.out.println("ERROR! Attribute Name \"" + tok +  "\" contains non-alphanumeric characters");
								return -1;
							}	
							attrNameList.add(tok);
							tok = parsedList1[index++];
							if (tok.endsWith(","))
							{
								tok = tok.replaceAll("[^A-Za-z0-9]", "");
								AttrType aType = getTokenType(tok);
								if (aType == null) 
								{
									System.out.println("ERROR! Unrecognized attribute type " + tok);
									return -1;
								}
								attrTypeList.add(aType);
							}
							else
							{
								if ((parsedList1[index++] + parsedList1[index++]).toLowerCase().matches("primarykey,|primarykey\\);|primarykey"))
								{
									tok = tok.replaceAll("[^A-Za-z0-9]", "");
									AttrType aType = getTokenType(tok);
									if (aType == null) 
									{
										System.out.println("ERROR! Unrecognized attribute type " + tok);
										return -1;
									}
									attrTypeList.add(aType);
								}
								else
								{
									System.out.println("ERROR! Expecting \',\' ");
									return -1;
								}
							}
							for (int i = index; i < parsedList1.length; i ++)
							{
								tok = parsedList1[i++];
								if (tok.equals(");"))
								{
									if (i == parsedList1.length)
										continue;
									else
									{
										System.out.println("ERROR! Not expecting input after \");\"");
										return -1;
									}
								}
								if (tok.matches(".*\\W+.*"))
								{
									if (tok.endsWith(");"))
									{
										System.out.println("ERROR! Expecting attribute type after attribute name " + tok.replaceAll("[^A-Za-z0-9]", ""));
										return -1;
									}
									System.out.println("ERROR! Attribute Name \"" + tok +  "\" contains non-alphanumeric characters");
									return -1;
								}
								else
								{
									attrNameList.add(tok);
								}
								if (i == parsedList1.length - 1)
								{
									tok = parsedList1[i];				
									if (!tok.endsWith(");"))
									{
										System.out.println("ERROR! Expecting query ending with \");\"");
										return -1;
									}
								}
								else
								{
									tok = parsedList1[i];
									
									if (i != parsedList1.length - 2 && !tok.endsWith(","))
									{
										System.out.println("ERROR! Expecting \',\' after " + tok);
										return -1;
									}
								}
								
								tok = tok.replaceAll("[^A-Za-z0-9]", "");
								AttrType aType = getTokenType(tok);
								if (aType == null) {
									System.out.println("ERROR! Unrecognized attribute type " + tok);
									return -1;
								}
								attrTypeList.add(aType);
							}	
						}
						else
						{
							System.out.println("ERROR! Expecting ( in front of " + tok);
							return -1;
						}
					}
				}
				else
				{
					System.out.println("ERROR! Table Name cannot start with  \'" + tableName.charAt(0) + "\'");
					return -1;
				}
				CreateTable.createTable(tableName, attrNameList, attrTypeList);
			}
			else if (tok.toLowerCase().equals("index"))
			{
				queryType[1] = true;
				if (parsedList1.length < 9)
				{
					System.out.println("ERROR! CREATE INDEX Query is missing necessary fields!");
					return -1;
				}
				else if (parsedList1.length > 9)
				{
					System.out.println("ERROR! CREATE INDEX Query has extra number of fields!");
					return -1;
				}
				indexName = parsedList1[index++];
				if (indexName.toLowerCase().equals("on"))
				{
					System.out.println("ERROR! Missing index name before \"ON\"");
					return -1;
				}
				else if (indexName.matches(".*\\W+.*"))
				{
					System.out.println("ERROR! Index name \"" + indexName + "\" contains non-alphanumeric characters");
					return -1;
				}
				tok = parsedList1[index++];
				if (!tok.toLowerCase().equals("on"))
				{
					System.out.println("ERROR! Expect \"ON\" after index name " + indexName);
					return -1;
				}
				tableName = parsedList1[index++];
				tok = parsedList1[index++];
				if (!tok.toLowerCase().equals("("))
				{
					System.out.println("ERROR! Expect \"(\" after table name " + tableName);
					return -1;
				}
				indexKeyName = parsedList1[index++];
				tok = parsedList1[index++];
				if (!tok.toLowerCase().equals(")"))
				{
					System.out.println("ERROR! Expect \')\' after index key name " + indexKeyName);
					return -1;
				}
				tok = parsedList1[index];
				if (tok.endsWith(";"))
				{
					tok = tok.substring(0, tok.length() - 1);
				}
				else
				{
					System.out.println("ERROR! Query should end with \";\"");
					return -1;
				}
				indexKeyType = getTokenType(tok);
				if (indexKeyType == null)
				{
					System.out.println("ERROR! Index key type \"" + indexKeyType + "\" is not recognized");
					return -1;
				}
				CreateIndex.createIndex(indexName, tableName, indexKeyName, indexKeyType);
			}
			else
			{
				System.out.println("ERROR! \"" + tok + "\"" + " can not be recognized.");
				return -1;
			}
			
		}
		else if (tok.toLowerCase().equals("insert"))
		{
			queryType[2] = true;
			if (parsedList1.length < 10)
			{
				System.out.println("ERROR! INSERT INTO Query is missing necessary fields!");
				return -1;
			}
			tok = parsedList1[index++];
			if (!tok.toLowerCase().equals("into"))
			{
				System.out.println("ERROR! Expecting \"INTO\" after \"INSERT\"");
				return -1;
			}
			tableName = parsedList1[index++];
			if (tableName.equals("("))
			{
				System.out.println("ERROR! Missing table name before \'(\'");
				return -1;
			}
			tok = parsedList1[index++];
			if (!tok.equals("("))
			{
				System.out.println("ERROR! Expecting \'(\' after table name \"" + tableName +"\"");
				return -1;
			}
			int counter = 0;
			int rectCounter = 0;
			ArrayList<Integer> rectRecord = new ArrayList<Integer>();
			tok = parsedList1[index++];
			while (!tok.equals(")"))
			{
				if (tok.toLowerCase().equals("values"))
				{
					System.out.println("ERROR! Expecting \')\' before \"VALUES\"");
					return -1;
				}
				tok = tok.replaceAll("[^a-zA-Z0-9]", "");
				attrNameList.add(tok);
				counter ++;
				tok = parsedList1[index++];
			}
			tok = parsedList1[index++];
			if (!tok.toLowerCase().equals("values"))
			{
				System.out.println("ERROR! Expecting \"VALUES\" after \')\'");
				return -1;
			}
			tok = parsedList1[index++];
			if (!tok.toLowerCase().equals("("))
			{
				System.out.println("ERROR! Expecting \'(\' after \"VALUES\"");
				return -1;
			}
			tok = parsedList1[index++];
			while (!tok.equals(");"))
			{
				
				if (tok.equals("("))
				{
					String temp = "";
					tok = parsedList1[index++];
					while (!tok.equals(")"))
					{
						temp += tok;
						tok = parsedList1[index++];
					}
					attrValueList.add(temp);
					tok = parsedList1[index++];
					continue;
				}
				tok = tok.replaceAll(",", "");
				attrValueList.add(tok);
				tok = parsedList1[index++];
			}
			InsertInto insertObj = new InsertInto();
			try {
				insertObj.insertInto(tableName, attrNameList, attrValueList);
				rid = insertObj.rid;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (tok.toLowerCase().equals("select"))
		{
			tok = parsedList1[index++];
			if (tok.toLowerCase().equals("intersection"))
			{
				queryType[4] = true;
				tok = parsedList1[index++];	// expect "("
				tok = parsedList1[index++];
				tok = tok.replaceAll(",", "");
				rectNameList.add(tok);
				tok = parsedList1[index++];
				rectNameList.add(tok);
				tok = parsedList1[index++];	// expect ")"
				tok = parsedList1[index++];	// expect "FROM"
				tok = parsedList1[index++];
				tableName = tok.replaceAll(",", "");
				tok = parsedList1[index++];
				tableName2 = tok;
				tok = parsedList1[index++];	// expect "WHERE"
				tok = parsedList1[index++];
				tok = tok.replaceAll(",", "");
				attrNameList.add(tok);
				tok = parsedList1[index++];	// expect "="
				attrOpList.add(Parser.getTokenOpType(tok));
				tok = parsedList1[index++];
				attrValueList.add(tok);
				tok = parsedList1[index++];	// expect "AND"
				tok = parsedList1[index++];
				attrNameList.add(tok);
				tok = parsedList1[index++];	// expect "="
				attrOpList.add(Parser.getTokenOpType(tok));
				tok = parsedList1[index++];
				tok = tok.replaceAll(";", "");
				attrValueList.add(tok);
				intersection = SelectQuery.selectIntersection(tableName, tableName2, rectNameList, attrNameList, attrOpList, attrValueList);
			}
			else if (tok.toLowerCase().equals("distance"))
			{
				queryType[5] = true;
				tok = parsedList1[index++];	// expect "("
				tok = parsedList1[index++];
				tok = tok.replaceAll(",", "");
				rectNameList.add(tok);
				tok = parsedList1[index++];
				rectNameList.add(tok);
				tok = parsedList1[index++];	// expect ")"
				tok = parsedList1[index++];	// expect "FROM"
				tok = parsedList1[index++];
				tableName = tok.replaceAll(",", "");
				tok = parsedList1[index++];
				tableName2 = tok;
				tok = parsedList1[index++];	// expect "WHERE"
				tok = parsedList1[index++];
				tok = tok.replaceAll(",", "");
				attrNameList.add(tok);
				tok = parsedList1[index++];	// expect "="
				attrOpList.add(Parser.getTokenOpType(tok));
				tok = parsedList1[index++];
				attrValueList.add(tok);
				tok = parsedList1[index++];	// expect "AND"
				tok = parsedList1[index++];
				attrNameList.add(tok);
				tok = parsedList1[index++];	// expect "="
				attrOpList.add(Parser.getTokenOpType(tok));
				tok = parsedList1[index++];
				tok = tok.replaceAll(";", "");
				attrValueList.add(tok);
				distance = SelectQuery.selectDistance(tableName, tableName2, rectNameList, attrNameList, attrOpList, attrValueList);
			}
			else
			{
				queryType[3] = true;
				while (!tok.toLowerCase().equals("area"))
				{
					tok = tok.replaceAll(",", "");
					attrNameList.add(tok);
					tok = parsedList1[index++];
				}
				tok = parsedList1[index++];	// expect "("
				tok = parsedList1[index++];
				rectNameList.add(tok);
				tok = parsedList1[index++];	// expect ")"
				tok = parsedList1[index++];	// expect "FROM"
				tok = parsedList1[index++];
				tok = tok.replaceAll(";", "");
				tableName = tok;
				SelectQuery.selectArea(tableName, attrNameList, rectNameList);
			}
		}
		else
		{
			System.out.println("ERROR! \"" + tok + "\"" + " can not be recognized.");
			return -1;
		}
		return 1;
	}

	private static AttrOperator getTokenOpType(String tok)
	{
		if (tok.toLowerCase().equals("="))
		{
			return new AttrOperator(0);
		}
		else if (tok.toLowerCase().equals("<"))
		{
			return new AttrOperator(1);
		}
		else if (tok.toLowerCase().equals(">"))
		{
			return new AttrOperator(2);
		}
		else if (tok.toLowerCase().equals("!="))
		{
			return new AttrOperator(3);
		}
		else if (tok.toLowerCase().equals("<="))
		{
			return new AttrOperator(4);
		}
		else if (tok.toLowerCase().equals(">="))
		{
			return new AttrOperator(5);
		}
		else
			return null;
	}
	
	private static AttrType getTokenType(String tok) {
		// TODO Auto-generated method stub
		if (tok.toLowerCase().equals("string"))
		{
			return new AttrType(0);
		}
		else if (tok.toLowerCase().equals("integer"))
		{
			return new AttrType(1);
		}
		else if (tok.toLowerCase().equals("real"))
		{
			return new AttrType(2);
		}
		else if (tok.toLowerCase().equals("symbol"))
		{
			return new AttrType(3);
		}
		else if (tok.toLowerCase().equals("rect"))
		{
			return new AttrType(4);
		}
		else
		{
			return null;
		}
	}
	
	private static String typeToString(AttrType type)
	{
		if (type.attrType == 0)
		{
			return "string";
		}
		else if (type.attrType == 1)
		{
			return "integer";
		}
		else if (type.attrType == 2)
		{
			return "real";
		}
		else if (type.attrType == 3)
		{
			return "symbol";
		}
		else if (type.attrType == 4)
		{
			return "rect";
		}
		else
		{
			return null;
		}
	}
	
	private static String opToString(AttrOperator op)
	{
		if (op.attrOperator == 0)
		{
			return "=";
		}
		else if (op.attrOperator == 1)
		{
			return "<";
		}
		else if (op.attrOperator == 2)
		{
			return ">";
		}
		else if (op.attrOperator == 3)
		{
			return "!=";
		}
		else if (op.attrOperator == 4)
		{
			return "<=";
		}
		else if (op.attrOperator == 5)
		{
			return ">=";
		}
		else
			return null;
	}
	
	private static void queryTypeInit()
	{
		for (int i = 0; i < queryType.length; i ++)
		{
			queryType[i] = false;
		}
	}
	
	public static String rebuiltQuery()
	{
		String rebuilted  = "";
		if (queryType[0])
		{
			rebuilted = "CREATE TABLE " + tableName + " (";
			for (int i = 0; i < attrTypeList.size(); i ++)
			{
				rebuilted += attrNameList.get(i) + " " + Parser.typeToString(attrTypeList.get(i)) + ", ";
			}
			rebuilted = rebuilted.substring(0, rebuilted.length() - 2);
			return rebuilted + ");";
		}
		else if (queryType[1])
		{
			return rebuilted = "CREATE INDEX " + indexName + " ON " + tableName + " ( " + indexKeyName + " ) " + indexKeyType;
		}
		else if (queryType[2])
		{
			rebuilted = "INSERT INTO " + tableName + " ( ";
			for (int i = 0; i < attrNameList.size(); i ++)
			{
				rebuilted += attrNameList.get(i) + ", ";
			}
			rebuilted = rebuilted.substring(0, rebuilted.length() - 2);
			rebuilted += " ) VALUES ( ";
			for (int i = 0; i < attrValueList.size(); i ++)
			{
				rebuilted += attrValueList.get(i) + ", ";
			}
			rebuilted = rebuilted.substring(0, rebuilted.length() - 2);
			return rebuilted + " );";
		}
		else if (queryType[3])
		{
			rebuilted  = "SELECT ";
			for (int i = 0; i < attrNameList.size(); i ++)
			{
				rebuilted += attrNameList.get(i) + ", ";
			}
			rebuilted = rebuilted.substring(0, rebuilted.length() - 2);
			rebuilted += " AREA ( " + rectNameList.get(0) + " ) FROM " + tableName + ";";
			
			return rebuilted;
		}
		else if (queryType[4])
		{
			rebuilted  = "SELECT INTERSECTION ( " + rectNameList.get(0) +  ", " + rectNameList.get(1) 
			+ " ) FROM " + tableName + ", " + tableName2 + " WHERE " + attrNameList.get(0) + " " + Parser.opToString(attrOpList.get(0))+ " "
			+ attrValueList.get(0) + " AND " + attrNameList.get(1) + " " + Parser.opToString(attrOpList.get(1)) + " " + attrValueList.get(1) + ";";
			return rebuilted;
		}
		else if (queryType[5])
		{
			rebuilted  = "SELECT DISTANCE ( " + rectNameList.get(0) +  ", " + rectNameList.get(1) 
			+ " ) FROM " + tableName + ", " + tableName2 + " WHERE " + attrNameList.get(0) + " " + Parser.opToString(attrOpList.get(0))+ " "
			+ attrValueList.get(0) + " AND " + attrNameList.get(1) + " " + Parser.opToString(attrOpList.get(1))+ " " + attrValueList.get(1) + ";";
			return rebuilted;
		}
		else
		{
			return null;
		}
		
	}
	
	
	/* Parser tests.
	public static void main(String[] args)
	{
		SystemDefs sysdef = new SystemDefs("test",100,100,"Clock");
		String query1 = "create table test1 ( id integer primary key, name string, location rect );";
		String query11 = "create table test2 ( id integer primary key, sid integer, name string, shape rect );";
		String query2 = "create index indextest on test1 ( location ) rect;";
		String query3 = "insert into test1 ( id, name, location ) values ( 1, sanzhang, ( 0.5, 1.0, 1.5, 2.0 ) );";
		String query31 = "insert into test1 ( id, name, location ) values ( 2, lisi, ( 1.0, 1.0, 3.0, 4.0 ) );";
		String query32 = "insert into test2 ( id, sid, name, location ) values ( 1, 101, tempe, ( 5.0, 4.0, 5.5, 6.0 ) );";
		String query33 = "insert into test2 ( id, sid, name, location ) values ( 1, 201, phoenix, ( 0.0, 3.0, 1.5, 4.0 ) );";
		String query4 = "select intersection ( location, location ) from test1, test1 where id = 1 and id = 2;";
		String query5 = "select distance ( location, location ) from test1, test1 where id = 1 and id = 2;";
		String query6 = "select id, name, area ( location ) from test1;";
		String query41 = "select intersection ( location, shape ) from test1, test2 where id = 2 and sid = 201;";
		String query51 = "select distance ( location, shape ) from test1, test2 where id = 2 and sid = 201;";
		if (Parser.parseQuery(query1) > 0)
		{
			System.out.println(Parser.rebuiltQuery());
		}
		if (Parser.parseQuery(query3) > 0)
		{
			System.out.println(Parser.rebuiltQuery());
		}
		Parser.parseQuery(query2);
		Parser.parseQuery(query11);
		Parser.parseQuery(query31);
		Parser.parseQuery(query32);
		Parser.parseQuery(query33);
		Parser.parseQuery(query5);
		Parser.parseQuery(query51);
	}
	*/
	
}
