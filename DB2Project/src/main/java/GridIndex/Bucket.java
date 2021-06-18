package GridIndex;

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
import java.util.Vector;

public class Bucket implements Serializable {
	
	private Vector<recordReference> references;
	
	private int maximumSize;
	private int itemsInBucket = 0;
	
	private String bucketName;
	
	private Bucket next = null;
	
	
	private static int currentBucketCount = 0;
	
	public Bucket() {
		references = new Vector<recordReference>();
		
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
		this.maximumSize = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
		
		currentBucketCount++;
		this.bucketName = "Bucket" + currentBucketCount;
		
		this.saveBucket();
	}
	
	
	public Vector<recordReference> getReferences() {
		return references;
	}


	public int getMaximumSize() {
		return maximumSize;
	}


	public int getItemsInBucket() {
		return this.references.size();
	}


	public String getBucketName() {
		return bucketName;
	}


	public Bucket getNext() {
		return next;
	}


	public void addRecordReference(recordReference r) {
		if(this.getItemsInBucket() < this.maximumSize) {
			references.add(r);
			this.sort();
		}
		else {
			if(next == null) 
				next = new Bucket();
	
			next.addRecordReference(r);
			next.sort();
		}
	}
	
	public void deleteReference(int index) {
		references.remove(index);
	}
	
	public int linearSearch(recordReference ref) {
		for(int i = 0; i < references.size(); i++) {
			recordReference x = references.get(i);
			if(ref.getPosition() == x.getPosition() && ref.getPageName().equals(x.getPageName()) && compareObjects(ref.getValue(), x.getValue()) == 0) {
				return i;
			}
		}
		return -1;
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
	
	public void saveBucket(){
		try {
	         FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + bucketName + ".ser");
	         
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(this);
	         out.close();
	         fileOut.close();
	      } 
		
		catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	
	public void sort() {
		Collections.sort(references);
	}
	
}
