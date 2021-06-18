package Main;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Vector;

import GridIndex.Bucket;
import GridIndex.Grid;
import GridIndex.recordReference;

public class DBApp implements DBAppInterface, Serializable{
	
	
	private static final long serialVersionUID = 1L;
	
	
	private transient Vector <Table> DBtables;
	
	private static String mainDir = "src/main/resources/data";
	private String DataBaseDir = "";
	
	@Override
	public void init() {
		DBtables = new Vector<Table>();
				
		File dbDir = new File(DataBaseDir = mainDir);
		if(!dbDir.exists())
			dbDir.mkdirs();
//		new File(DataBaseDir + "data").mkdirs();
	}
		
	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		
		// 1- Validate column types
		boolean flag = ValidateDataTypes(colNameType);
		if(!flag) {
			System.out.println("Not accepted data type");
			return;
		}
		
		//2- check table name doesn't exist
		File f = new File("src/main/resources/data/" + tableName + ".ser");
//		if(f.exists()) {
//			throw new DBAppException("Table with same name already exists");
//		}
		
		try {
			addMetaData(tableName, colNameType, clusteringKey, colNameMin, colNameMax);
			
			//Create table and save it
			Table t = new Table(tableName);
			t.setHtblColNameType(colNameType);
			t.setHtblColNameMax(colNameMax);
			t.setHtblColNameMin(colNameMin);
			t.setClusteringKey(clusteringKey);
			t.saveTable();
			
			//Add table to DB
			DBtables.add(t);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void addMetaData(String tableName, Hashtable colNameType, String clusteringKey, Hashtable colNameMin, Hashtable colNameMax) throws IOException{
		File metadata = new File("src/main/resources/metadata.csv");
		PrintWriter pr = new PrintWriter(new FileWriter(metadata, true));
		
		Enumeration<String> enumeration = colNameType.keys();
		 while(enumeration.hasMoreElements()) {
	            String colName = enumeration.nextElement();
	            String colType = (String) colNameType.get(colName);
	            boolean isClusteringKey = clusteringKey.equals(colName);
	            String min = (String) colNameMin.get(colName);
	            String max = (String) colNameMax.get(colName);
	            pr.append(tableName + "," + colName + "," + colType + "," + isClusteringKey + "," + "False," + min + "," + max + "\n");
	        }
		 pr.flush();
		 pr.close();	
	}
	
	public static boolean ValidateDataTypes(Hashtable <String,String> h) {
		ArrayList<String> acceptedDataTypes = new ArrayList<String>();
		acceptedDataTypes.add("java.lang.Integer");
		acceptedDataTypes.add("java.lang.String");
		acceptedDataTypes.add("java.lang.Double");
		acceptedDataTypes.add("java.util.Date");
		
		for(int i = 0; i < h.size(); i++) {
			String dataType = h.values().toArray()[i].toString();
			System.out.println(dataType);
			if(!acceptedDataTypes.contains(dataType)) {
				return false;
			}
		}
		return true;
	}
	
	public boolean checkValidInput(Hashtable <String,Object> colNameValue, Table t) {
		for (Entry<String, Object> entry : colNameValue.entrySet()) {
			String colName = entry.getKey();
			Object value = entry.getValue();
			
			if(!t.getHtblColNameType().containsKey(colName)) {
				return false;
			}
			
			String colType = t.getHtblColNameType().get(colName);
			if(colType.equals("java.lang.Integer") && !(value instanceof Integer)) {
				return false;
			}
				
			if(colType.equals("java.lang.Double") && !(value instanceof Double)) {
				return false;
			}
				
			if(colType.equals("java.lang.String") && !(value instanceof String)) {
				return false;
			}
				
			if(colType.equals("java.util.Date") && !(value instanceof Date)) {
				return false;
			}	
		}
		return true;
	}
	
	public static Table deserializeTable(String tableName) {
		Table t = null;
		try {
	         FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".ser");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         t = (Table) in.readObject();
	         in.close();
	         fileIn.close();
	      } 
		catch (IOException i) {
	         i.printStackTrace();
	      } 
		catch (ClassNotFoundException c) {
	         System.out.println("Employee class not found");
	         c.printStackTrace();
	      }
		
		return t;
	}
	
	public static Page deserializePage(String pageName) {
		Page p = null;
		try {
	         FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + pageName + ".ser");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         p = (Page) in.readObject();
	         in.close();
	         fileIn.close();
	      } 
		catch (IOException i) {
	         i.printStackTrace();
	      } 
		catch (ClassNotFoundException c) {
	         System.out.println("Page class not found");
	         c.printStackTrace();
	      }
		
		return p;
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		Table t = deserializeTable(tableName);
		if(t == null)
			throw new DBAppException("Table doen't exist");
		
		Grid g = new Grid(columnNames.length, columnNames, tableName);
		g.firstInserts();
		
		System.out.println(t.getGrids().size());
		t.getGrids().add(g);
		
		if(g.getNumberOfDimensions() == 1) {
			t.getSingleDGrids().put(g.getColNames()[0], g);
		}
		
		t.saveTable();
		System.out.println(t.getGrids().size());
		System.out.println("grid created and added");
	}
	
	public void insertRecordInGrids(Page p, Record r, Table t) {
		
		int row = p.binarySearch(r.getRecord(), t.getHtblColNameType(), t.getClusteringKey(), t.getHtblColNameType().get(t.getClusteringKey()));
		if(row == -1)
			return;
		ArrayList <Integer>temp = bsHelper(row, p, r.getRecord(), t.getHtblColNameType(), t.getClusteringKey());
		row = temp.get(0);
		
		Vector<Grid> grids = t.getGrids();
		for(int i = 0; i < grids.size(); i++) {
			Grid g = grids.get(i);
			
			Object value = r.getRecord().get(g.getColNames()[0]);
			
			recordReference ref = new recordReference(p.getName(), row, value);
			g.insertRecord(r, t, ref);
		}
	}
	
	@SuppressWarnings("unused")
	@Override
	
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		
		Table t = deserializeTable(tableName);
		//System.out.println(t.getName());
		if(t == null)
			throw new DBAppException("Table doen't exist");
		
		boolean f = checkValidInput(colNameValue, t);
		
        if(!f || colNameValue.get(t.getClusteringKey()) == null)
            throw new DBAppException("Data violation");
        
        
//		//check validity of input data
		try {
			boolean isValidData = checkMetaData(tableName, colNameValue);
			if(!isValidData) {
				return;
			}
			
			
		} 
		catch (IOException | ParseException e) {
			System.out.println(e.getMessage());
			return;
		}
		System.out.println(t.getName());
		
		String clusteringCol = t.getClusteringKey();
		String clusteringColType = t.getHtblColNameType().get(clusteringCol);
		
		Record r = new Record(clusteringCol, colNameValue);
		
		Object value = colNameValue.get(clusteringCol);
		
//		System.out.println(value);
		Vector <String> pages = t.getTablePages();
		
		Page p = deserializePage(pages.get(0));
		if(p.getItemsInPage() == 0) {
			p.addRecord(r);
			p.savePage();
			insertRecordInGrids(p,r,t);
			return;
		}
		
		for(int i = 0; i < pages.size(); i++) {
			p = deserializePage(pages.get(i));
			
			if(pages.size() == i+1) {  //Last page
				if(p.getItemsInPage() < p.getMaximumSize()) { //not full, so just insert
					p.addRecord(r);
					p.sort();
					p.savePage();
					insertRecordInGrids(p,r,t);
					return;
				}
				else if(p.getItemsInPage() == p.getMaximumSize()){ //full, so create new page, and add last to new page
					p.addRecord(r);
					p.sort();
					Record last = p.getRecords().lastElement();
					p.deleteRecord(200);
					p.savePage();
					
					Page newPage = new Page();
					newPage.addRecord(last);
					
//					t.addPage(newPage);
					t.getTablePages().add(i+1, newPage.getName());
					newPage.savePage();
					t.saveTable();
					insertRecordInGrids(p,r,t);
					return;	
				}
			}
			
			else if(i == 0 && p.getItemsInPage() == p.getMaximumSize() && compareRecords(clusteringColType, r.getCkValue(), p.getMinRecord()) <= 0) {
				Page newPage = new Page();
				newPage.addRecord(r);
				newPage.savePage();
				
				t.getTablePages().add(i, newPage.getName());
				p.savePage();
				t.saveTable();
				insertRecordInGrids(p,r,t);
				return;
			}
						
			else if(p.getItemsInPage() < p.getMaximumSize()) { //Not full page + not last page				
				Page next = deserializePage(pages.get(i+1));
				if(compareRecords(clusteringColType, r.getCkValue(), next.getMinRecord()) < 0) {
					p.addRecord(r);
					p.sort();
					p.savePage();
					insertRecordInGrids(p,r,t);
					return;
				}
			}
			
			else if(p.getItemsInPage() == p.getMaximumSize()) {
				Page next = deserializePage(pages.get(i+1));
				
				if((compareRecords(clusteringColType, r.getCkValue(), p.getMinRecord()) >= 0 && compareRecords(clusteringColType, r.getCkValue(), p.getMaxRecord()) < 0) || compareRecords(clusteringColType, r.getCkValue(), p.getMinRecord()) < 0) {
					p.addRecord(r);
					p.sort();
					Record last = p.getRecords().lastElement();
					p.deleteRecord(p.getRecords().size() - 1);
					p.savePage();
					
					if(next.getItemsInPage() < p.getMaximumSize()) {
						next.addRecord(last);
						next.sort();
						
						next.savePage();
						insertRecordInGrids(p,r,t);
						return;
					}
					else {
						Page newPage = new Page();
						newPage.addRecord(last);
						
						t.getTablePages().add(i+1, newPage.getName());
						newPage.savePage();
						t.saveTable();
						insertRecordInGrids(p,r,t);
						return;
					}
				}
			}
		}
	}
	
	public static double compareRecords(String clusteringColType, Object r1, Object r2) {
		switch(clusteringColType) {
		case "java.lang.Integer": return (Integer)r1 - (Integer)r2;
			
		case "java.lang.String": return ((String)r1).compareTo((String)r2);
			
		case "java.lang.Double": return (Double)r1 - (Double)r2;
			
		case "java.util.Date": return ((Date)r1).compareTo((Date)r2);
			
		default: return 0.0001;
		}
	}
	
	public boolean checkMetaData(String tableName, Hashtable colNameValue) throws IOException, ParseException {
		BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String row = "";
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(",");
		    if(data[0].equals(tableName)) {
		    	if(colNameValue.get(data[1]) == null) {
		    		continue;
		    	}
		    	switch(data[2]) {
		    	case "java.lang.Integer": int value1 = (Integer) colNameValue.get(data[1]);
		    							  int min1 = Integer.parseInt(data[5]);
		    							  int max1 = Integer.parseInt(data[6]);
		    							  if(value1 < min1 || value1 > max1) {
		    								  return false;
		    							  }
		    							  continue;
		    	
		    	case "java.lang.String": String value2 = (String)colNameValue.get(data[1]);
		    							 String min = data[5];
		    							 String max = data[6];
		    							 if(value2.compareTo(min) < 0 || value2.compareTo(max) > 0) {
		    								 return false;
		    							 }
		    							 continue;
		    	
		    	case "java.lang.Double": double value3 = (Double)colNameValue.get(data[1]);
		    							 double min3 = Double.parseDouble(data[5]);
		    							 double max3 = Double.parseDouble(data[6]);
		    							  if(value3 < min3 || value3 > max3) {
		    								  return false;
		    							  }
		    							  continue;
		    	
		    	case "java.util.Date": //String sDate1 = (String)colNameValue.get(data[1]);  
		        					   //Date date = new SimpleDateFormat("yyyy-mm-dd");
		    						   Date date = (Date)colNameValue.get(data[1]);
		        					   Date min4 = new SimpleDateFormat("yyyy-MM-dd").parse(data[5]);
		        					   Date max4 = new SimpleDateFormat("yyyy-MM-dd").parse(data[6]);
		        					   //System.out.println(date + "-" + min4);
		        					   if(date.compareTo(min4) < 0 || date.compareTo(max4) > 0) {
		    								 return false;
		        					   }
		        					   continue;
		        					   
		    	default: return false;
		    	}	
		    }
		}
		csvReader.close();
		
		return true;
	}
	
	public static Object extractValue(String value, String colType) {
		switch(colType) {
		case "java.lang.Integer": return Integer.parseInt(value);
		
		case "java.lang.String": return value;
			
		case "java.lang.Double": return Double.parseDouble(value);
			
		case "java.util.Date": try {
				return new SimpleDateFormat("yyyy-MM-dd").parse(value);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		default: return null;
		}
	}
	
	public int findIndex(Table t, Hashtable<String,Object> colNameValue) {
		ArrayList<String> columnNames = new ArrayList<String>();
		for (Entry<String, Object> entry : colNameValue.entrySet()) {
			columnNames.add(entry.getKey());
		}
		
		int x = getGridIndex(t, columnNames);
		if(x != -1) {
			Grid g = t.getGrids().get(x);
			for(String s : g.getColNames()) {
				if(!columnNames.contains(s))
					return -1;
			}
			return x;
		}
		return -1;
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		Table t = deserializeTable(tableName);
        if(t == null)
            throw new DBAppException("Table doen't exist");
        String clusteringColName = t.getClusteringKey();
        String clusteringColType = t.getHtblColNameType().get(clusteringColName);
        
        Object value = extractValue(clusteringKeyValue, clusteringColType);
        columnNameValue.put(clusteringColName, value);
        
        boolean f = checkValidInput(columnNameValue, t);
		
        if(!f || columnNameValue.get(t.getClusteringKey())==null)
            throw new DBAppException("Data violation");
        
        
//		//check validity of input data
		try {
			boolean isValidData = checkMetaData(tableName, columnNameValue);
			if(!isValidData) {
				return;
			}
		}
        
		catch (IOException | ParseException e) {
			System.out.println(e.getMessage());
			return;
		}
		
		// use the index if found
		
		Grid g = t.getSingleDGrids().get(clusteringColName);
		if(g != null) {
			int x = g.bucketIndex(columnNameValue, t);
			Bucket b = deserializeBucket(g.getGrid()[x]);
			Vector<recordReference> references = b.getReferences();
			Object val = columnNameValue.get(g.getColNames()[0]);
			Vector<recordReference> res = new Vector<recordReference>();
			
			for(recordReference ref : references) {
				if(compareObjects(val, ref.getValue()) == 0) {
					res.add(ref);
				}
			}
			
			for(recordReference ref : references) {
				String pageName = ref.getPageName();
				int row = ref.getPosition();
				Page p = deserializePage(pageName);
				
				Record old = p.getRecords().remove(row);
				
				Record r = new Record(clusteringColName, columnNameValue);
				p.getRecords().add(row, r);
				
				updateRecordInGrids(p, old, t,r);
				
				p.savePage();
				return;
			}
			
		}
        
		else {
			Vector <String> pages = t.getTablePages();
	        for(int i = 0; i <pages.size(); i++) {
	    		Page p = deserializePage(pages.get(i));
	    		if(compareRecords(clusteringColType, value, p.getMinRecord())>=0 && compareRecords(clusteringColType, value, p.getMaxRecord())<=0) {
	    			int index = p.binarySearch(columnNameValue, t.getHtblColNameType(), clusteringColName, clusteringColType);
	    			
	    			if(index == -1) {
	    				System.out.println("Record not found");
	    				continue;
	    			}
	    			
	    			Record oldRecord = p.getRecords().get(index);
	    			
	    			p.getRecords().remove(index);
	    			Record r = new Record(clusteringColName, columnNameValue);
	    			p.getRecords().add(index, r);
	    			
	    			updateRecordInGrids(p, oldRecord, t,r);
	    			
	    			p.savePage();
	    			return;
	    		}	
	        }
		}
        
	}
	
	public void updateRecordInGrids(Page p, Record r, Table t, Record r1) {
		
		int row = p.binarySearch(r.getRecord(), t.getHtblColNameType(), t.getClusteringKey(), t.getHtblColNameType().get(t.getClusteringKey()));
		
		ArrayList <Integer>temp = bsHelper(row, p, r.getRecord(), t.getHtblColNameType(), t.getClusteringKey());
		Vector<Grid> grids = t.getGrids();
//		row = temp.get(0);
		for(int j = 0; j < temp.size(); j++) {
			row = temp.get(j);
			for(int i = 0; i < grids.size(); i++) {
				Grid g = grids.get(i);

				Object value = null;
				value = r.getRecord().get(g.getColNames()[0]);
				
				recordReference ref = new recordReference(p.getName(), row, value);

				value = r1.getRecord().get(g.getColNames()[0]);
				recordReference newRef = new recordReference(p.getName(), row, value);
				g.updateRecord(ref, r, t, newRef, r1);
			}
		}
	}
	

	
	public void deleteRecordInGrids(Page p, Record r, Table t) {
		
		int row = p.binarySearch(r.getRecord(), t.getHtblColNameType(), t.getClusteringKey(), t.getHtblColNameType().get(t.getClusteringKey()));
		
		ArrayList <Integer>temp = bsHelper(row, p, r.getRecord(), t.getHtblColNameType(), t.getClusteringKey());
		Vector<Grid> grids = t.getGrids();
//		row = temp.get(0);
		for(int j = 0; j < temp.size(); j++) {
			row = temp.get(j);
			for(int i = 0; i < grids.size(); i++) {
				Grid g = grids.get(i);

				Object value = r.getRecord().get(g.getColNames()[0]);
				recordReference ref = new recordReference(p.getName(), row, value);
				g.deleteRecord(ref,r,t);
			}
		}
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table t = deserializeTable(tableName);
        if(t == null)
            throw new DBAppException("Table doen't exist");
        
        boolean f = checkValidInput(columnNameValue, t);
        if(!f) {
        	throw new DBAppException("Data violation");
        }
        
        String clusteringColName = t.getClusteringKey();
        String clusteringColType = t.getHtblColNameType().get(clusteringColName);

        
        int x = findIndex(t, columnNameValue);
        if(x != -1) {
        	Grid g = t.getGrids().get(x);
        	x = g.bucketIndex(columnNameValue, t);
        	
        	Bucket b = deserializeBucket(g.getGrid()[x]);
        	Vector<recordReference> references = b.getReferences();
        	Object value = columnNameValue.get(g.getColNames()[0]);
        	Vector<recordReference> temp = new Vector<recordReference>();
        	for(recordReference ref : references) {
        		if(compareObjects(value, ref.getValue()) == 0)
        			temp.add(ref);
        	}
        	
        	for(recordReference ref : temp) {
        		String pageName = ref.getPageName();
        		int row = ref.getPosition();
        		Page p = deserializePage(pageName);
        		Record r = p.getRecords().get(row);
        		
        		boolean found = true;
    			for (Entry<String, Object> entry : columnNameValue.entrySet()) {
    				if(compareRecords(t.getHtblColNameType().get(entry.getKey()), entry.getValue(), r.getRecord().get(entry.getKey())) != 0) {
    					found = false;
    				}
    			}
    			if(found) {
    				p.deleteRecord(row);
    				deleteRecordInGrids(p,r,t);
    				
    				if(p.getItemsInPage() <= 0) {
            			t.getTablePages().remove(t);
            			t.saveTable();	
            		}
            		p.savePage();
    			}
        	}
        	return;
        }
        
        else {
        	Object value = columnNameValue.get(clusteringColName);
            Vector <String> pages = t.getTablePages();
            if(value == null) {
            	for(int i = 0; i < pages.size(); i++) {
            		Page p = deserializePage(pages.get(i));
            		ArrayList <Integer> indices = p.linearSearch(columnNameValue,t.getHtblColNameType());
            		for(int j = 0; j<indices.size(); j++) {
            			Record r = p.getRecords().get(indices.get(j));
            			p.deleteRecord(indices.get(j));
            			deleteRecordInGrids(p,r,t);
            		}
            		
            		if(p.getItemsInPage() <= 0) {
            			t.getTablePages().remove(i);
            			t.saveTable();	
            			return;
            		}
            		
            		p.savePage();
            	}
            }
            else {
            	for(int i = 0; i < pages.size(); i++) {
            		Page p = deserializePage(pages.get(i));
            		int index = p.binarySearch(columnNameValue, t.getHtblColNameType(), clusteringColName, clusteringColType);
            		if(index == -1) {
            			System.out.println("Record not found");
            			continue;
            		}
            		if(compareRecords(clusteringColType, value, p.getMinRecord())>=0 && compareRecords(clusteringColType, value, p.getMaxRecord())<=0) {
            			ArrayList <Integer> indices = bsHelper(index,p,columnNameValue, t.getHtblColNameType(),clusteringColName);
            			for(int j = 0; j<indices.size(); j++) {
            				Record r = p.getRecords().get(indices.get(j));
            				deleteRecordInGrids(p,r,t);
                			p.deleteRecord(indices.get(j));
                		}
            			
            			if(p.getItemsInPage() <= 0) {
                			t.getTablePages().remove(i);
                			t.saveTable();
                			return;
                		}
            			
                		p.savePage();
            		}
            	}
            }
        }
	}
	public static ArrayList<Integer> bsHelper(int index, Page p, Hashtable <String,Object> colNameValue, Hashtable <String,String> colNameType, String clusteringCol) {
		ArrayList <Integer> res = new ArrayList<Integer>();
		Vector <Record> records = p.getRecords();
		Object value = colNameValue.get(clusteringCol);
		String clusteringColType = colNameType.get(clusteringCol);
		for(int i = index; i<records.size(); i++) {
			Record r = records.get(i);
			boolean found = true;
			for (Entry<String, Object> entry : colNameValue.entrySet()) {
				if(compareRecords(colNameType.get(entry.getKey()), entry.getValue(), r.getRecord().get(entry.getKey())) != 0) 
					found = false;
			}
			if(found)
				res.add(i);
			if(compareRecords(clusteringColType, value, r.getCkValue()) != 0)
				break;
		}
		for(int i = index-1; i >= 0; i--) {
			Record r = records.get(i);
			boolean found = true;
			for (Entry<String, Object> entry : colNameValue.entrySet()) {
				if(compareRecords(colNameType.get(entry.getKey()), entry.getValue(), r.getRecord().get(entry.getKey())) != 0) 
					found = false;
			}
			if(found)
				res.add(i);
			if(compareRecords(clusteringColType, value, r.getCkValue()) != 0)
				break;
		}
		return res;
	}
	
	public static Bucket deserializeBucket(String BucketName) {
		Bucket b = null;
		try {
	         FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + BucketName + ".ser");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         b = (Bucket) in.readObject();
	         in.close();
	         fileIn.close();
	      } 
		catch (IOException i) {
	         i.printStackTrace();
	      } 
		catch (ClassNotFoundException c) {
	         System.out.println("bucket class not found");
	         c.printStackTrace();
	      }
		
		return b;
	}
	
	public double compareObjects(Object o1, Object o2) {
		if(o1 instanceof Integer) {
			return (Integer)o1 - (Integer)o2;
		}
		else if(o1 instanceof Double) {
			return (Double)o1 - (Double)o2;
		}
		else if(o1 instanceof Date) {
			return ((Date)o1).compareTo((Date)o2);
		}
		else if(o1 instanceof String) {
			return ((String)o1).compareTo((String)o2);
		}
		return 0;
	}
	
	
	public Vector<Record> selectEqualNoIdx(SQLTerm query) throws DBAppException {
		Vector <Record> res = new Vector<Record>();
		Object value = query.get_objValue();
		
		Table t = deserializeTable(query.get_strTableName());
		Vector<String> pages = t.getTablePages();
		for(String name : pages) {
			Page p = deserializePage(name);
			Vector <Record> records = p.getRecords();
			for(Record r : records) {
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) == 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> selectNotEqualNoIdx(SQLTerm query) throws DBAppException {
		Vector <Record> res = new Vector<Record>();
		Object value = query.get_objValue();
		
		Table t = deserializeTable(query.get_strTableName());
		Vector<String> pages = t.getTablePages();
		for(String name : pages) {
			Page p = deserializePage(name);
			Vector <Record> records = p.getRecords();
			for(Record r : records) {
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) != 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> selectLessThanNoIdx(SQLTerm query) throws DBAppException {
		Vector <Record> res = new Vector<Record>();
		Object value = query.get_objValue();
		
		Table t = deserializeTable(query.get_strTableName());
		Vector<String> pages = t.getTablePages();
		for(String name : pages) {
			Page p = deserializePage(name);
			Vector <Record> records = p.getRecords();
			for(Record r : records) {
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) < 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> selectLessorEqlThanNoIdx(SQLTerm query) throws DBAppException {
		Vector <Record> res = new Vector<Record>();
		Object value = query.get_objValue();
		
		Table t = deserializeTable(query.get_strTableName());
		Vector<String> pages = t.getTablePages();
		for(String name : pages) {
			Page p = deserializePage(name);
			Vector <Record> records = p.getRecords();
			for(Record r : records) {
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) <= 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> selectGreaterThanNoIdx(SQLTerm query) throws DBAppException {
		Vector <Record> res = new Vector<Record>();
		Object value = query.get_objValue();
		
		Table t = deserializeTable(query.get_strTableName());
		Vector<String> pages = t.getTablePages();
		for(String name : pages) {
			Page p = deserializePage(name);
			Vector <Record> records = p.getRecords();
			for(Record r : records) {
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) > 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> selectGreaterThanorEqlNoIdx(SQLTerm query) throws DBAppException {
		Vector <Record> res = new Vector<Record>();
		Object value = query.get_objValue();
		
		Table t = deserializeTable(query.get_strTableName());
		Vector<String> pages = t.getTablePages();
		for(String name : pages) {
			Page p = deserializePage(name);
			Vector <Record> records = p.getRecords();
			for(Record r : records) {
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) >= 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> executeNoIdx(SQLTerm query) throws DBAppException{
		Vector <Record> res = new Vector<Record>();
		
		switch(query.get_strOperator()) {
		case "=": res = selectEqualNoIdx(query); break;
		case "!=": res = selectNotEqualNoIdx(query); break;
		case "<": res = selectLessThanNoIdx(query); break;
		case ">": res = selectGreaterThanNoIdx(query); break;
		case "<=": res = selectLessorEqlThanNoIdx(query); break;
		case ">=": res = selectGreaterThanorEqlNoIdx(query); break;
		}
		
		return res;
	}
	
	public Vector<Record> AND(Vector<Record> v1, Vector<Record> v2){
		Vector <Record> res = new Vector<Record>();
		
		for(Record r1 : v1) {
			for(Record r2 : v2) {
				if(r1.compareTo(r2) == 0) {
					System.out.println("Found it");
					res.add(r1);
					break;
				}
			}
		}
		return res;
	}
	
	public Vector<Record> OR(Vector<Record> v1, Vector<Record> v2){
		Vector <Record> res = new Vector<Record>();
		res.addAll(v1);
		
		for(Record r2 : v2) {
			boolean f = true;
			for(Record r : res) {
				if(r.compareTo(r2) == 0) {
					f = false;
					break;
				}
			}
			if(f)
				res.add(r2);
		}
		return res;
	}
	
	public Vector<Record> XOR(Vector<Record> v1, Vector<Record> v2){
		Vector <Record> res = new Vector<Record>();
		res.addAll(v1);
		
		for(Record r2 : v2) {
			for(Record r : res) {
				if(r.compareTo(r2) == 0) {
					res.remove(r);
					break;
				}
			}
		}
		return res;
	}
	
	public Vector<Record> doOperation(String op, Vector<Record> v1, Vector<Record> v2){
		Vector <Record> res = new Vector<Record>();
		switch(op) {
		case "AND": res = AND(v1,v2); break;
		case "OR": res = OR(v1,v2); break;
		case "XOR": res = XOR(v1,v2); break;
		}
		return res;
	}
	
	public int getGridIndex(Table t, ArrayList<String> columnNames) {
		Vector<Grid> grids = t.getGrids();
		Hashtable <Integer,Integer> h = new Hashtable<Integer, Integer>();
		
		for(int i = 0; i < grids.size(); i++) {
			int weight = 0;
			String [] cols = grids.get(i).getColNames();
			
			for(int j = 0; j < cols.length; j++) {
				if(columnNames.contains(cols[j])) {			
					weight++;
				}
			}
			h.put(i, weight);
		}
		
		int maxKey = -1;
		int maxValue = 0; 
		

		for(Entry<Integer, Integer> entry : h.entrySet()) {
			if(entry.getValue() == maxValue && maxKey != -1) {
		    	if(grids.get(maxKey).getColNames().length > grids.get(entry.getKey()).getColNames().length) {
		    		maxValue = entry.getValue();
			        maxKey = entry.getKey();
		    	}
		     }
			else if(entry.getValue() > maxValue) {
		         maxValue = entry.getValue();
		         maxKey = entry.getKey();
		     }
		}
				
		if(maxValue == 0)
			return -1;
		
		return maxKey;
	}
		
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{
		Table t = deserializeTable(sqlTerms[0].get_strTableName());
		
		if(t == null)
			throw new DBAppException("No table with that name");
		
		Vector <Record> res = new Vector<Record>();
		
		Vector<Vector<Record>> temp = new Vector<Vector<Record>>();
		
		ArrayList <SQLTerm> terms = new ArrayList<SQLTerm>();
		for(SQLTerm term : sqlTerms)
			terms.add(term);
		
		ArrayList<String> operators = new ArrayList<String>();
		for(String op : arrayOperators)
			operators.add(op);
		
		if(terms.size() >= 2) {
			SQLTerm term1 = terms.remove(0);
			SQLTerm term2 = terms.remove(0);
			String operator = operators.remove(0);
			ArrayList<String> columnNames = new ArrayList<String>();
			columnNames.add(term1.get_strColumnName());
			columnNames.add(term2.get_strColumnName());
			
			int x = getGridIndex(t, columnNames);
			Grid g = null;
			boolean flag = false;
			
			if(term1.get_strOperator().equals("!=") || term2.get_strOperator().equals("!="))
				flag = true;
			
			if(x != -1) {
				g = t.getGrids().get(x);
				for(String colName : g.getColNames()) {
					if(!columnNames.contains(colName)) {
						flag = true;
						break;
					}
				}
				if(g.getNumberOfDimensions() != 2)
					flag = true;
			}
			
			boolean flag2 = true;
			if(t.getSingleDGrids().get(term1.get_strColumnName()) != null || t.getSingleDGrids().get(term2.get_strColumnName()) != null)
				flag2 = false;
			
			if(x == -1 && flag && flag2) {
				Vector<Record> v1 = executeNoIdx(term1);
				Vector<Record> v2 = executeNoIdx(term2);
				res = doOperation(operator, v1, v2);
			}
			
			else if(x != -1 && !flag && flag2){
				if(term1.get_strOperator().equals("=") && term2.get_strOperator().equals("=") && operator.equals("AND")) {
					ArrayList<SQLTerm> tempTerms = new ArrayList<SQLTerm>();
					tempTerms.add(term1);
					tempTerms.add(term2);
					res = g.NDExactAND(tempTerms, t);
				}
				else {
					terms.add(term1);
					terms.add(term2);
					operators.add(operator);
				}
			}
			
			else {
				terms.add(term1);
				terms.add(term2);
				operators.add(operator);
			}
		}
		
		while(!terms.isEmpty()) {
			SQLTerm term = terms.remove(0);
			Grid g = t.getSingleDGrids().get(term.get_strColumnName());
			
			if(g == null || term.get_strOperator().equals("!=")) {
				Vector <Record> v = executeNoIdx(term);
				System.out.println("v is " + v.toString());
				temp.add(v);
			}
			else {
				Vector <Record> v = g.singleQuery(term);
				temp.add(v);
			}
		}
		
		if(temp.size() > 0)
			res = temp.remove(0);
		
		while(!operators.isEmpty()) {
			Vector<Record> v1 = temp.remove(0);
			res = doOperation(operators.remove(0), v1, res);
		}
		
		return res.iterator();
	}
	
	public static void main(String[] args) {
		Table t = deserializeTable("students");
//		System.out.println(p.getRecords());
		
		Grid g = t.getGrids().get(3);
		System.out.println(Arrays.toString(g.getGrid()));
		System.out.println(Arrays.toString(g.getColNames()));
		
		Bucket b = deserializeBucket("Bucket431");
		System.out.println(b.getReferences());
	}
}