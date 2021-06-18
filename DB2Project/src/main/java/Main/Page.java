package Main;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Vector;

public class Page implements Serializable{
	
	private int maximumSize;
	
	private static int currentPageCount = 0;
	
	private Vector<Record> records;
	private int itemsInPage;
	
	private String name;
	
	private Object minRecord = null;
	private Object maxRecord = null;
	
	public Object getMinRecord() {
		return records.get(0).getCkValue();
	}

	public void setMinRecord(Object minRecord) {
		this.minRecord = minRecord;
	}

	public Object getMaxRecord() {
		return records.get(records.size() - 1).getCkValue();
	}

	public void setMaxRecord(Object maxRecord) {
		this.maxRecord = maxRecord;
	}
		
	public Page(){
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
		    is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
		   
		}
		try {
		    prop.load(is);
		} catch (IOException ex) {
		    
		}
		this.maximumSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		
		currentPageCount++;
		this.name = "page" + currentPageCount;
		
		records = new Vector<Record>();
		itemsInPage = records.size();
		
		this.savePage();
	}
	
	public int getMaximumSize() {
		return maximumSize;
	}
	
	public int getItemsInPage() {
		return records.size();
	}

	public static int getCurrentPageCount() {
		return currentPageCount;
	}

	public Vector<Record> getRecords() {
		return records;
	}

	public String getName() {
		return name;
	}
	
	public void addRecord(Record h) {
		this.records.add(h);
		itemsInPage++;
	}
	
	public void deleteRecord(int index) {
		this.records.removeElementAt(index);
		itemsInPage--;
	}
	
	public int binarySearch(Hashtable <String, Object> colNameValue, Hashtable <String, String> colNameType ,String ckColName, String ckColType) {
        int l = 0;
        int r = records.size()-1;
        Object targetValue = colNameValue.get(ckColName);

        while (l <= r) {
            int m = l + (r - l) / 2;

            Object midValue = records.get(m).getCkValue();

            double x = compareRecords(ckColType, targetValue, midValue);

            if(x == 0) {
				return m;
            }

            else if(x > 0) {
                l = m + 1;
            }

            else if(x < 0) {
                r = m - 1;
            }
        }
        return -1;
    }

	
	public ArrayList <Integer> linearSearch(Hashtable <String, Object> colNameValue, Hashtable <String, String> colNameType) {
		ArrayList <Integer> res = new ArrayList<Integer>();
		for(int i = 0; i < records.size(); i++) {
			Hashtable <String, Object> record = records.get(i).getRecord();
			boolean found = true;
			for (Entry<String, Object> entry : colNameValue.entrySet()) {
				if(compareRecords(colNameType.get(entry.getKey()), entry.getValue(), record.get(entry.getKey())) != 0) {
					found = false;
				}
			}
			if(found)
				res.add(i);
		}
		return res;
	}
	
	
	public void savePage(){
		try {
	         FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + name + ".ser");
	         
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(this);
	         out.close();
	         fileOut.close();
	      } 
		
		catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	
	public static double compareRecords(String clusteringColType, Object r1, Object r2) {
		//System.out.println(r1 + "  " + r2);
		switch(clusteringColType) {
		case "java.lang.Integer": return (Integer)r1 - (Integer)r2;
			
		case "java.lang.String": return ((String)r1).compareTo((String)r2);
			
		case "java.lang.Double": return (Double)r1 - (Double)r2;
			
		case "java.util.Date": return ((Date)r1).compareTo((Date)r2);
			
		default: return 0.0001;
		}
	}
	
	public void sort() {
		Collections.sort(records);
	}
}