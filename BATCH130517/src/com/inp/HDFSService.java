package com.inp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.InputStream;

public class HDFSService extends Configured implements Tool {
	
	public static void main(String[] args) {

		// step 1 : Check Input argument is given or no
		if (args.length < 2)
			{
				System.out.println("Java Usage "+HDFSService.class.getName()+"  [configuration] /path/to/edge/node//local/file/path/to/hdfs/destination");
				return;
			}
		
		// step 2 : Load COnfiguration
		Configuration conf=new Configuration(Boolean.TRUE);
		
		conf.set("fs.defaultFs","hdfs://localhost:8020");
		
		// step 3 : ToolRunner.run
		System.out.println("BEFORE TOOLRUNNER");
		
		
		try {
			System.out.println("TO RUN");
			int i=ToolRunner.run(conf, new HDFSService(), args);
			
		
			if(i==0) {
				System.out.println("SUCCESS");
			} else {
				System.out.println("FAILURE"+"    " + i);
			}
			
		} catch (Exception e){
			System.out.println("FAILURE IN EXCEPTION HANDLING");
			e.printStackTrace();
		}
		
		
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		FSDataOutputStream fos;
		
		System.out.println("in RUN");
		Configuration conf = super.getConf();
		FileSystem hdfs = FileSystem.get(conf);
		
		// Input
		final String input=args[0]; //wordcount.txt
		System.out.println("args[0]"+args[0]);
		
		
		final String hdfsOutPutDir=args[1]; //user/training
		System.out.println("args[1]"+args[1]);
		
		// File Write = create metadata + add data
		
		// creating MetaDAta = create empty fiel
		final Path FileNAmeWithDEstinationLocation = new Path(hdfsOutPutDir, input); // "user/training/wordcount.txt
		System.out.println(FileNAmeWithDEstinationLocation);
		
				
		
		Boolean i = hdfs.exists(FileNAmeWithDEstinationLocation);
		
		System.out.println("BOOLEAN i = " +i);
		
		if (i==Boolean.FALSE) { 
			System.out.println("CREATING FILE");
			fos =hdfs.create(FileNAmeWithDEstinationLocation);
		} else{
			fos = hdfs.append(FileNAmeWithDEstinationLocation);
			
		}
		
		System.out.println("before input stream");
		InputStream is = new FileInputStream(input);
		
		IOUtils.copyBytes(is, fos, conf, Boolean.TRUE);
		
		return 0;
	}

	
	}
