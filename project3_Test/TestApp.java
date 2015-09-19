package project3_Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class TestApp {

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
		HashMap<Character, Integer> hm = new HashMap<Character , Integer>();
		HashMap<Character, Integer> hg = new HashMap<Character , Integer>();
		HashMap<Integer,Integer> readMissed = new HashMap<Integer , Integer>();
		HashMap<Integer,Integer> writeMissed = new HashMap<Integer , Integer>();
		System.out.println("========= \n");
		 FileReader file = null;
		 String line = "";
		int noOfNodes=0;
		      file = new FileReader("Configuration.txt");
		      BufferedReader reader = new BufferedReader(file);
		    
		      	
		      	  try {
					while ((line = reader.readLine()) != null) 
					  {  //System.out.println(line);
					  if(!line.startsWith("#"))
					  {
						  noOfNodes=Integer.parseInt(line.trim());
					      System.out.println(noOfNodes);
					      break;
						  
					  }
					  }
				} catch (NumberFormatException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		ConfigReader rd = new ConfigReader(noOfNodes);
		boolean[] listRead = new boolean[noOfNodes];
		boolean[] listWrite = new boolean[noOfNodes];
		rd.readFile("Configuration.txt");
		int totalOps =rd.noOfOperations;
		int expected_read_lines = rd.noOfRead;
		//int fracOfWrite = 100 - rd.noOfNodes * rd.noOfWrite;
		int expected_write_lines = rd.noOfWrite;
		System.out.println("number of read lines expected : "+ expected_read_lines) ;
		System.out.println("number of write lines expected : "+ expected_write_lines) ;
		System.out.println("total no. of lines expected::"+ (expected_read_lines+expected_write_lines));
		//int expected_count = expected_read_lines+expected_write_lines;
		System.out.println("========= \n");
		int count = 0;
		int errRead = 0;
		int errWrite = 0;
		boolean error[] = new boolean[rd.noOfNodes];
		int cnt =0;
		
		for(int i=0;i<rd.noOfNodes;i++)
			error[i] = false;
		
		
		//ArrayList<Integer> readMissed = new ArrayList<Integer>();
		//ArrayList<Integer> writeMissed = new ArrayList<Integer>();
		//boolean CSvoilated = false;
		for(int i=0;i< rd.noOfNodes;i++)
		{
		file = new FileReader("NodeHost"+i+".txt");
		reader = new BufferedReader(file);
		  String[] array = new String[100];
		  int readlineCount = 0,writelineCount=0;
      	  try {
			while ((line = reader.readLine()) != null && !line.isEmpty()) 
			  {
			  //System.out.println(line);
			  count++;
			  array = line.split(" ");
			  //to track errors in read and write ops
			  
			  if(hg.containsKey(array[1].charAt(0)) && line.contains("read") && line.contains("Error"))
			  { 
				  error[i]= true;
				  cnt = hg.get(array[1].charAt(0)); 
				  
				  hg.put(array[1].charAt(0),++cnt);
				  ++errRead;
				 // System.out.println(i + " err read " + errRead);
			  
			  }
			  else if(!hg.containsKey(array[1].charAt(0)) && line.contains("write") && line.contains("Error"))
			  {
				  error[i] = true;
				  hg.put(array[1].charAt(0), 1);
				  errWrite = 1;
				  //System.out.println(i + " err read " + errWrite);
			  }
			  else if(!hg.containsKey(array[1].charAt(0)) && line.contains("read") && line.contains("Error"))
			  {
				  error[i] = true;
				  hg.put(array[1].charAt(0), 1);
				  errRead = 1;
				  //System.out.println(i + " err read " + errRead);
			  }
			  else if(hg.containsKey(array[1].charAt(0)) && line.contains("write") && line.contains("Error"))
			  {
				  cnt = hg.get(array[1].charAt(0));
				  hg.put(array[1].charAt(0),++cnt);
				  ++errWrite;
				  //System.out.println(i + " err read " + errWrite);
				  error[i] = true;
			  }
			  
			  //to track properly executed read/write ops
			  else if(hm.containsKey(array[0].charAt(0)) && line.contains("read") && !line.contains("Error"))
			  { //CSvoilated= false;
				   cnt = hm.get(array[0].charAt(0)); 
				  hm.put(array[0].charAt(0),++cnt);
				  readlineCount++;
				  //System.out.println(i + "  read " + readlineCount);	
			  
			  }
			  else if(!hm.containsKey(array[0].charAt(0)) && line.contains("write") && !line.contains("Error"))
			  {
				 // CSvoilated = true;
				  hm.put(array[0].charAt(0), 1);
				  writelineCount = 1;
				 // System.out.println(i + "  read " + writelineCount);	
			  }
			  else if(!hm.containsKey(array[0].charAt(0)) && line.contains("read") && !line.contains("Error"))
			  {
				 // CSvoilated = true;
				  hm.put(array[0].charAt(0), 1);
				  readlineCount = 1;
				  //System.out.println(i + "  read " + readlineCount);
			  }
			  else if(hm.containsKey(array[0].charAt(0)) && line.contains("write") && !line.contains("Error"))
			  {
				  cnt = hm.get(array[0].charAt(0));
				  hm.put(array[0].charAt(0),++cnt);
				  ++writelineCount;
				 // System.out.println(i + "  read " + writelineCount);
				  //CSvoilated = true;
			  }
			    }
			if((readlineCount+errRead)!=expected_read_lines)
			{
				listRead[i]=true;
				readMissed.put(i, ((rd.noOfRead-(readlineCount+errRead))));
				//System.out.println(i + "  no of read " + rd.noOfRead + " sum " + (readlineCount+errRead));
			}
			if((writelineCount+errWrite)!=expected_write_lines)
			{
				listWrite[i]=true;
				writeMissed.put(i, (rd.noOfWrite-(writelineCount+errWrite)));
				//System.out.println(i + "  no of write " + rd.noOfWrite + " sum " + (writelineCount+errWrite));
			}
			readlineCount=0;
			errRead=0;
			writelineCount=0;
			errWrite=0;
			cnt=0;
		} catch(Exception e){e.printStackTrace();}
      	  
      	  
		}
		
		
		for(Character c : hm.keySet())
		{
			System.out.println("Number of read/write operations satisified by Host number " + c + " is ===> "+ hm.get(c));
			
			//HostInfo h = rd.hosts.get(c);
		}
		
		
		for(Character c : hg.keySet())
		{
		System.out.println("Number of read/write operations not satisified by Host number " + c + " is ===> "+ hg.get(c));
		}
		
		System.out.println("Total number of read/write operations : " + count);
	    	
		    for(int j=0;j<noOfNodes;j++)
		    {
	    	if(listWrite[j] || error[j]){
	    		System.out.println(" Number of write reqs not satisified for node "+j+" is "+writeMissed.get(j));
		    }if(listRead[j] || error[j])
		    {
		    	System.out.println(" Number of read reqs not satisified for node "+j+" is "+readMissed.get(j));
		    }
	    	else if(!error[j] && totalOps==count){
		    	System.out.println(" Success... all read/write requests are satisfied for node "+j);
		    	
		    }
		    }
	    	
	    
	}

}
