package GridIndex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import Main.DBAppException;
import Main.Page;
import Main.Record;
import Main.SQLTerm;
import Main.Table;


public class Grid implements Serializable {
	
	private String[] grid; //bucket names
	
	private String tableName;
	
	private ArrayList<Range[]> ranges = new ArrayList<Range[]>();
	
	private int numberOfDimensions;
	private String[] colNames;
	
	public Grid(int n, String[] colNames, String tableName) {
		numberOfDimensions = n;
		this.colNames = colNames;
		
		this.tableName = tableName;
		
		grid = new String[(int)Math.pow(10, n)];
		
		for(int i = 0; i < grid.length; i++) {
			Bucket b = new Bucket();
			grid[i] = b.getBucketName();
			b.saveBucket();
		}
        
        for(int i = 0; i < colNames.length; i++) {
        	this.createRanges(tableName, colNames[i]);
        }
	}

	//create ranges for a column
	public void createRanges(String tableName, String colName) {
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String row = "";
			Object min = null ,max = null;
			while ((row = csvReader.readLine()) != null) {
			    String[] data = row.split(",");
			    if(data[0].equals(tableName) && data[1].equals(colName)) {
			    	switch(data[2]) {
			    	case "java.lang.Integer": min = new Integer(Integer.parseInt(data[5]));
			    							  max = new Integer(Integer.parseInt(data[6]));
			    							  break;
			    	
			    	case "java.lang.String" : min = new String(data[5]);
			    							 max = new String(data[6]);
//									    	 min = "AAAAAA";
//											 max = "zzzzzz";
			    							 break;
			    	
			    	case "java.lang.Double": min = new Double(Double.parseDouble(data[5]));
			    							 max = new Double(Double.parseDouble(data[6]));
			    							 break;
			    	
			    	case "java.util.Date": min = new SimpleDateFormat("yyyy-MM-dd").parse(data[5]);
					   					   max = new SimpleDateFormat("yyyy-MM-dd").parse(data[6]);
					   					   break;
			    	}
			    }
			}
//			System.out.println(min+ " --> " + max);
			// divide into 10 ranges
			Range [] arr = new Range [10];
			if(min instanceof Integer) {
				int difference = (Integer)max - (Integer)min;
				int x = (int) Math.ceil(difference/10);
				
				int low = (Integer)min;
				int high = new Integer((Integer)min + x);
				
				
				for(int i = 1; i <= 10; i++) {
					if(high > (Integer)max) {
						high = (Integer)max;
					}
					Range r = new Range(low,high);
					arr[i-1] = r;
					low = high + 1;
					high = new Integer(low + x);
				}
				ranges.add(arr);
			}
			
			if(min instanceof Double) {
				double difference = (Double)max - (Double)min;
				double x = difference/10;
				
//				System.out.println(x);
				
				double low = (Double)min;
				double high = new Double((Double)min + x);
				
				
				for(int i = 1; i <= 10; i++) {
					if(high > (Double)max) {
						high = (Double)max;
					}
					Range r = new Range(low,high);
					arr[i-1] = r;
					low = high;
					high = new Double(low + x);
				}
				ranges.add(arr);
			}
			
			if(min instanceof String) {
				String s1 = (String)min;
				String s2 = (String)max;
				
				// Case id xx-xxxx
				if((s1.charAt(2) + "").equals("-")) {
					int min1 = Integer.parseInt(s1.substring(0,2));
					int max1 = Integer.parseInt(s2.substring(0,2));
					
					String min2 = s1.substring(3);
					String max2 = s2.substring(3);
					
					int difference = max1 - min1;
					int m = (int) Math.ceil(difference / 10);
					
					int l = min1;
					int h = min1 + m;
					
					for(int i = 1; i <= 10; i++) {
						if(h > max1) {
							h = max1;
						}
						
						if(i == 10) {
							h = max1;
						}
						
						String u = l + "-" + min2;
						String v = h + "-" + max2;
						Range r = new Range(u,v);
						arr[i-1] = r;
						l = h + 1;
						h = l + m;
					}
					ranges.add(arr);
					return;
				}
				
				int [] x = new int[s1.length()];
				for(int i = 0; i < s1.length(); i++) {
					int y = s2.charAt(i) - s1.charAt(i);
					x[i] = (int) Math.ceil(y / 10);
				}
				
//				System.out.println(Arrays.toString(x));
				String low = s1;
				String high = helper1(s1,x);
				for(int i = 1; i <= 10; i++) {
					if(high.compareTo((String)max) > 0) {
						high = s2;
					}
					
					if(i == 10) {
						high = s2;
					}
					
					Range r = new Range(low, high);
					arr[i-1] = r;
					low = high;
					high = helper1(low, x);
				}
				ranges.add(arr);
			}
			
			if(min instanceof Date) {
				LocalDate dateBefore = ((Date) min).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
				LocalDate dateAfter = ((Date)max).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
				long noOfDaysBetween = ChronoUnit.DAYS.between(dateBefore, dateAfter);
				long x = (long) Math.ceil(noOfDaysBetween/10);
				
				Date low = Date.from(dateBefore.atStartOfDay(ZoneId.systemDefault()).toInstant());
				LocalDate temp = dateBefore.plusDays(x);
				Date high = Date.from(temp.atStartOfDay(ZoneId.systemDefault()).toInstant());
				
				for(int i = 1; i <= 10; i++) {
					if(i == 10) {
						high = (Date)max;
					}
					
					Range r = new  Range(low, high);
					arr[i-1] = r;
					low = high;
					temp = low.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
					temp = temp.plusDays(x);
					high = Date.from(temp.atStartOfDay(ZoneId.systemDefault()).toInstant());
				}
				ranges.add(arr);
			}
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public String helper1(String s, int [] x) {
		String res = "";
		for(int i = 0; i < x.length; i++) {
			char c = (char)(s.charAt(i) + x[i]);
			res += c;
		}
		return res;
	}	
	
	public ArrayList<Range[]> getRanges() {
		return ranges;
	}

	public String[] getGrid() {
		return grid;
	}

	public String[] getColNames() {
		return colNames;
	}

	public int getNumberOfDimensions() {
		return numberOfDimensions;
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
	         System.out.println("Table class not found");
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
	
	public void firstInserts() throws DBAppException {
		Table t = deserializeTable(tableName);
		if(t == null)
			throw new DBAppException("Table doen't exist");
		
		Vector <String> pages = t.getTablePages();
		for(int i = 0; i < pages.size(); i++) {
			Page p = deserializePage(pages.get(i));
			Vector <Record> records = p.getRecords();
			for(int j = 0; j < records.size(); j++) {
				Record r = (Record) records.get(j);
				Hashtable h = r.getRecord();
				
				Object value = r.getRecord().get(this.getColNames()[0]);
				recordReference ref = new recordReference(p.getName(), j, value);
				int x = bucketIndex(h, t);
				String bucketName = grid[x];
				
				Bucket b = deserializeBucket(bucketName);
				b.addRecordReference(ref);
				b.sort();
				b.saveBucket();
			}
		}
	}
	
	public void insertRecord(Record r, Table t, recordReference ref) {
		int x = bucketIndex(r.getRecord(), t);
		String bucketName = grid[x];
		
		Bucket b = deserializeBucket(bucketName);
		b.addRecordReference(ref);
		b.sort();
		b.saveBucket();
	}
	
	public void deleteRecord(recordReference ref, Record r, Table t) {
		int x = bucketIndex(r.getRecord(), t);
		String bucketName = grid[x];
		Bucket b = deserializeBucket(bucketName);
		while(true) {
			int index = b.linearSearch(ref);
			if(index == -1) {
				b = b.getNext();
				if(b == null)
					return;
				continue;
			}
			b.deleteReference(index);
			b.saveBucket();
			return;
		}
	}
	
	public void updateRecord(recordReference ref, Record r, Table t, recordReference newRef, Record r1) {
		this.deleteRecord(ref, r, t);
		this.insertRecord(r1, t, newRef);
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

	
	public Vector<Record> singleQueryEqual(SQLTerm query, Bucket b){
		Vector <Record> res = new Vector<Record>();
		Vector<recordReference> references = b.getReferences();
		Object value = query.get_objValue();
		for(recordReference ref : references) {
			Page p = deserializePage(ref.getPageName());
			Record r = p.getRecords().get(ref.getPosition());
			Object recordValue = r.getRecord().get(query.get_strColumnName());
			if(compareObjects(recordValue, value) == 0)
				res.add(r);
		}
		return res;
	}
	
	public Vector<Record> singleQueryLessThan(SQLTerm query, int x){
		Vector <Record> res = new Vector<Record>();
		while(x >= 0) {
			Bucket b = deserializeBucket(grid[x]);
			Vector<recordReference> references = b.getReferences();
			Object value = query.get_objValue();
			
			for(recordReference ref : references) {
				Page p = deserializePage(ref.getPageName());
				Record r = p.getRecords().get(ref.getPosition());
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) < 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> singleQueryLessThanorEqual(SQLTerm query, int x){
		Vector <Record> res = new Vector<Record>();
		while(x >= 0) {
			Bucket b = deserializeBucket(grid[x]);
			Vector<recordReference> references = b.getReferences();
			Object value = query.get_objValue();
			
			for(recordReference ref : references) {
				Page p = deserializePage(ref.getPageName());
				Record r = p.getRecords().get(ref.getPosition());
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) <= 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> singleQueryGreaterThan(SQLTerm query, int x){
		Vector <Record> res = new Vector<Record>();
		while(x < grid.length) {
			Bucket b = deserializeBucket(grid[x]);
			Vector<recordReference> references = b.getReferences();
			Object value = query.get_objValue();
			
			for(recordReference ref : references) {
				Page p = deserializePage(ref.getPageName());
				Record r = p.getRecords().get(ref.getPosition());
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) > 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> singleQueryGreaterThanorEqual(SQLTerm query, int x){
		Vector <Record> res = new Vector<Record>();
		while(x < grid.length) {
			Bucket b = deserializeBucket(grid[x]);
			Vector<recordReference> references = b.getReferences();
			Object value = query.get_objValue();
			
			for(recordReference ref : references) {
				Page p = deserializePage(ref.getPageName());
				Record r = p.getRecords().get(ref.getPosition());
				Object recordValue = r.getRecord().get(query.get_strColumnName());
				if(compareObjects(recordValue, value) >= 0)
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> NDExactAND(ArrayList<SQLTerm> terms, Table t){
		Vector <Record> res = new Vector<Record>();
		
		Hashtable <String, Object> h = new Hashtable<String, Object>();
		for(SQLTerm term : terms) {
			h.put(term.get_strColumnName(), term.get_objValue());
		}
		
		int x = bucketIndex(h, t);
		Bucket b = deserializeBucket(grid[x]);
		
		Vector<recordReference> references = b.getReferences();
		for(recordReference ref : references) {
			Page p = deserializePage(ref.getPageName());
			Record r = p.getRecords().get(ref.getPosition());
			for(SQLTerm term : terms) {
				Object value = term.get_objValue();
				Object recordvalue = r.getRecord().get(term.get_strColumnName());
				if(compareObjects(recordvalue, value) == 0 && !res.contains(r))
					res.add(r);
			}
		}
		return res;
	}
	
	public Vector<Record> singleQuery(SQLTerm query){
		Table t = deserializeTable(query.get_strTableName());
		
		Vector <Record> res = new Vector<Record>();
		Hashtable <String, Object> h = new Hashtable<String, Object>();
		h.put(query.get_strColumnName(), query.get_objValue());
		
		int x = bucketIndex(h, t);
		Bucket b = deserializeBucket(grid[x]);
		
		switch(query.get_strOperator()) {
		case "=": res = singleQueryEqual(query, b); break;
		case "<": res = singleQueryLessThan(query, x); break;
		case ">": res = singleQueryGreaterThan(query, x); break;
		case "<=": res = singleQueryLessThanorEqual(query, x); break;
		case ">=": res = singleQueryGreaterThanorEqual(query, x); break;
		}
		
		return res;
	}
	
	public int bucketIndex(Hashtable<String, Object> colNameValue, Table t) {
		int res = 0;
		
		for(int i = 0; i < ranges.size(); i++) {
			Object val = colNameValue.get(colNames[i]);
//			System.out.println(val);
			String colType = t.getHtblColNameType().get(colNames[i]);
			for(int j = 0; j < 10; j++) {
				if(compareRecords(colType, val, ranges.get(i)[j].getLowerLimit()) >= 0 && compareRecords(colType, val, ranges.get(i)[j].getUpperLimit()) <= 0 ) {
					int power = (int) Math.pow(10, i);
					power = j * power;
					res += power;
					break;
				}
			}
		}
		return res;
	}
	
	public static void main(String[] args) {		
		Bucket b = deserializeBucket("Bucket1");
		System.out.println(b.getItemsInBucket());
		System.out.println(b.getReferences().get(0));
	}
	
}
