package Main;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import GridIndex.Grid;

public class Table implements Serializable {
	
	private String name;
//	private Vector <Page> tablePages;
	private Vector <String> tablePages;
	private Hashtable<String, String> htblColNameType, htblColNameMin, htblColNameMax;
	private String clusteringKey;
	
	private Vector<Grid> grids;
	private Hashtable <String,Grid> singleDGrids = new Hashtable<String, Grid>();
	
	public Table(String name) throws IOException{
		this.name = name;
//		tablePages = new Vector <Page>();
		tablePages = new Vector <String>();
		htblColNameType = new Hashtable<String,String>();
		htblColNameMin = new Hashtable<String,String>();
		htblColNameMax = new Hashtable<String,String>();
		clusteringKey = "";
		
		grids = new Vector<Grid>();
		
		Page p = new Page();
//		tablePages.add(p);
		tablePages.add(p.getName());
	}
	
	
	public Hashtable<String, Grid> getSingleDGrids() {
		return singleDGrids;
	}


	public Vector<Grid> getGrids() {
		return grids;
	}



	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}

	public void setHtblColNameMin(Hashtable<String, String> htblColNameMin) {
		this.htblColNameMin = htblColNameMin;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	public void setHtblColNameMax(Hashtable<String, String> htblColNameMax) {
		this.htblColNameMax = htblColNameMax;
	}

	public String getClusteringKey() {
		return clusteringKey;
	}

	public void setClusteringKey(String clusteringKey) {
		this.clusteringKey = clusteringKey;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTablePages(Vector<String> tablePages) {
		this.tablePages = tablePages;
	}

	public void saveTable(){
		
		try {
	         FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + name + ".ser");
	         
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(this);
	         out.close();
	         fileOut.close();
	         
	         //System.out.println("Serialized data is saved in: database/" + name + ".ser");
	      } 
		
		catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	
	public void addPage(Page p) {
		this.tablePages.add(p.getName());
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
		this.htblColNameType = htblColNameType;
	}

	public String getName() {
		return name;
	}

	public Vector<String> getTablePages() {
		return tablePages;
	}
}
