import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.*;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

enum Cond{
		gt, ge, eq, ne, le, lt;
	}

class Filt{
	int filt_col;
	Cond cond;
	double filt_value;
	public Filt(int filt_col, Cond cond, double filt_value){
		this.filt_col = filt_col;
		this.cond = cond;
		this.filt_value = filt_value;
	}
}

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.util.*;
import java.util.regex.*;
import java.util.List;
import java.util.ArrayList;

enum Cond{
	gt,ge,eq,ne,le,lt;
}

class Filt{
	int filt_col;
	Cond cond;
	double filt_value;
	public Filt(int filt_col, Cond cond, double filt_value){
		this.filt_col = filt_col;
		this.cond = cond;
		this.filt_value = filt_value;
	}
}

public class Hw1Grp4{
	public static boolean judgement(Cond cond, String[] templine, Filt filt){
		double num = Double.valueOf(templine[filt.filt_col]);
		switch(cond){
			case gt:
				if(num > filt.filt_value)
					return true;
				else
					return false;
			case ge:
				if(num >= filt.filt_value)
					return true;
				else
					return false;
			case eq:
				if(num == filt.filt_value)
					return true;
				else
					return false;
			case ne:
				if(num != filt.filt_value)
					return true;
				else
					return false;
			case le:
				if(num <= filt.filt_value)
					return true;
				else
					return false;
			case lt:
				if(num < filt.filt_value)
					return true;
				else
					return false;
		}
		return false;
	}	
	

	public static void main(String[] args) throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException{
		//handling args
		String file = args[0].split("=")[1];
		//System.out.println(filepath);
		String filt_string =  args[1].split(":")[1];
		int filt_col = Integer.parseInt(filt_string.split(",")[0].substring(1));
		Cond cond = Cond.valueOf(filt_string.split(",")[1]);
		double filt_value = Double.valueOf(filt_string.split(",")[2]);
		Filt filt = new Filt(filt_col,cond,filt_value);
		//distict cols
		String[] rawDistictCols = args[2].split(":")[1].split(",");
		List<Integer> distictCols = new ArrayList<Integer> ();
		for(String e : rawDistictCols){
			distictCols.add(Integer.parseInt(e.substring(1)));
		} 
		//make hashtable
		Hashtable<String, String> ht = new Hashtable<String, String>();
		//Load file from HDFS
		Configuration conf = new Configuration();
        	FileSystem fs = FileSystem.get(URI.create(file), conf);
        	Path path = new Path(file);
        	FSDataInputStream in_stream = fs.open(path);
        	BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        	String s;
		while ((s=in.readLine())!=null) {
			String[] templine = s.split("[|]");
			if(judgement(cond, templine, filt) == true){
				String dataline = "";
				for(int i = 0; i < distictCols.size(); i++){
					dataline += templine[distictCols.get(i)];
					if(i < distictCols.size()-1){
						dataline += "|";
					}      		
				}
				//System.out.println(dataline);
				//写入hashtable
				ht.put(dataline, "");
			}
			else{
				continue;
			}
		}
		//System.out.println(ht.size());

		//Configure HBase
		//Logger.getRootLogger().setLevel(Level.WARN);

        // create table descriptor
        String tableName= "Result";
	    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
		// create column descriptor
		HColumnDescriptor cf = new HColumnDescriptor("res");
		htd.addFamily(cf);

		// configure HBase
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);

		if (hAdmin.tableExists(tableName)) {
		    System.out.println("Table already exists, Now deleting this table...");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
			System.out.println("Former table has been deleted, creating a new table...");
			hAdmin.createTable(htd);
		    System.out.println("table "+tableName+ " created successfully");
		}
		else {
		    hAdmin.createTable(htd);
		    System.out.println("table "+tableName+ " created successfully");
		}
		hAdmin.close();
		//put data in HBase
		Enumeration<String> data = ht.keys();
		HTable table = new HTable(configuration,tableName);
		int row_key = 0;
		while(data.hasMoreElements()){
			//insert in hbase
	 		String[] str = ((String)data.nextElement()).split("[|]");
			//add row number
			Put put = new Put(Integer.toString(row_key).getBytes());
			for(int i = 0; i < distictCols.size();i++){
				String col = "R"+Integer.toString(distictCols.get(i));
				String col_value = str[i];
				put.add("res".getBytes(),col.getBytes(),col_value.getBytes());
			}
			table.put(put);
			row_key++;
	    }
			table.close();
			System.out.println("put successfully");
        	in.close();
        	fs.close();
	}
}





