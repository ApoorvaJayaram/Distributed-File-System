package project3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class Node {
	
	ConfigReader rd;
	int nodeNum;
	int a=1, b=2, c;
	int noRead, noWrite;
	LockManager manager;
	final int MESSAGE_SIZE = 50000;
	Semaphore semaphore;
	ExpoGen eGen = new ExpoGen();
	int rOp, wOp;
	
	public Node(int num, ConfigReader r, Semaphore sem, LockManager mgr)
	{
		rd = r;
		nodeNum = num;
		semaphore = sem;
		manager = mgr;
	}
	
	public void execute()
	{
		boolean rDone,wDone;
		rDone = false;
		wDone = false;
		Random random = new Random();
		noRead = rd.noOfRead;
		noWrite = rd.noOfWrite;
		try {
			 
		      File f = new File("./Node" + "Host" + nodeNum + ".txt");
	 
		      if (!f.createNewFile())
		      {
		        System.out.println("File already exists.");
		      }
		      
		      for(int i=0;i<rd.noOfOperations;i++)
		      {
		    	  c = random.nextBoolean() ? a : b;
		    	  if(c==1)
		    	  {
		    		 
		    		 System.out.println("calling performRead from "+nodeNum);
		    			  performRead(f,i);
		    			  System.out.println("perform read "+i+" node "+nodeNum);
		    			  noRead = noRead - 1;
		    			  long val = eGen.generateExpo(rd.meanDelay);
		    			  try {
							Thread.sleep(val);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		    			 if(noRead==0)
			    		  {
			    			  rDone = true;
			    			  wOp = i+1;
			    			  System.out.println(nodeNum + " completed all read operations");
			    			  break;
			    		  }
		    			  //manager.releaseLock("read");
		    	  }
		    	  else
		    	  if(c==2)
		    	  {
		    			  System.out.println("calling performWrite from "+nodeNum);
		    			  performWrite(f,i);
		    			  System.out.println("perform write "+i+" node "+nodeNum);
		    			  noWrite = noWrite - 1;
		    			  long val = eGen.generateExpo(rd.meanDelay);
		    			  try {
							Thread.sleep(val);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		    			  if(noWrite==0)
			    		  {
			    			  wDone = true; 
			    			  rOp = i+1;
			    			  System.out.println(nodeNum + " completed all write operations");
			    			  break;
			    		  }
		    			  //manager.releaseLock("write");
		    	  }
		    	  
		      }
		      
		      if(rDone == true && wDone!=true)
		      {
		    	  for(int j=wOp;j<rd.noOfOperations;j++)
		    	  {
		    		  System.out.println("all read done now calling performWrite from "+nodeNum);
		    		  performWrite(f,j);
		    		  System.out.println("perform write "+j+" node "+nodeNum);
		    		  long val = eGen.generateExpo(rd.meanDelay);
	    			  try {
						Thread.sleep(val);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    			  //manager.releaseLock("write");
		    	  }
		    	  wDone=true;
		    	  System.out.println(nodeNum + " completed all write operations");
		      }
		      if(wDone == true && rDone!=true)
		      {
		    	  for(int j=rOp;j<rd.noOfOperations;j++)
		    	  {
		    		  System.out.println("all write done now calling performRead from "+nodeNum);
		    		  performRead(f,j);
		    		  System.out.println("perform read "+j+" node "+nodeNum);
		    		  long val = eGen.generateExpo(rd.meanDelay);
	    			  try {
						Thread.sleep(val);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    			  //manager.releaseLock("read");
		    	  }
		    	  rDone=true;
		    	  System.out.println(nodeNum + " completed all read operations");
		      }
		      if(rDone == true && wDone==true)
		      {
		    	  System.out.println(nodeNum + " completed all read and write operations");
		      }
		      
	    	} 
			catch (IOException e) 
			{
		      e.printStackTrace();
	    	}
	}
	
	public void performRead(File file, int count)
	{
		System.out.println("inside perform read "+nodeNum);
		Random rand = new Random();
		//manager = new LockManager(rd.hosts,nodeNum,rd,semaphore);
		int fileNo = rand.nextInt((rd.noOfFiles - 1) + 1) + 1;
		System.out.println(nodeNum + "has selected file no" + fileNo + " for read ");
		//int fileNo = 1;
		System.out.println("sending read request by " + nodeNum);
		boolean b = manager.sendRequest("Read", fileNo);
			try
			{
				//PrintWriter writer = new PrintWriter(file,true);
				//writer.println(nodeNum + " executed read request no " + count);
				FileWriter fileWritter = new FileWriter(file,true);
		        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
		        PrintWriter writer = new PrintWriter(bufferWritter);
		        if(b)
		        {
			        System.out.println(nodeNum + " executed read request no " + count + " from file: " + fileNo + " at time: " + System.currentTimeMillis());
			        writer.println(nodeNum + " executed read request no " + count + " from file: " + fileNo + " at time: " + System.currentTimeMillis());
			        manager.releaseLock("read",fileNo);
			        writer.close();
		        }
		        else
		        {
		        	System.out.println(nodeNum + " couldnt execut read request no " + count);
		        	writer.println("Error " + nodeNum + " could not execute read request no " + count + " from file: " + fileNo + " at time: " + System.currentTimeMillis());
			        //writer.println(nodeNum + " couldnt execute read request no " + count);
			        //manager.releaseLock("read",fileNo);
			        writer.close();
		        }
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
	}
	
	public void performWrite(File logFile, int count)
	{
		System.out.println("inside perform write "+nodeNum);
		Random rand = new Random();
		//manager = new LockManager(rd.hosts,nodeNum,rd,semaphore);
		int fileNo = rand.nextInt((rd.noOfFiles - 1) + 1) + 1;
		System.out.println(nodeNum + "has selected file no" + fileNo + " for write ");
		//int fileNo = 1;
		System.out.println("sending write request by " + nodeNum);
		boolean b = manager.sendRequest("write", fileNo);
		
		String name = rd.hosts.get(nodeNum).hostName;
		File file = new File("./root/" + name + "/" + Integer.toString(fileNo) + ".txt");
		
		try
		{
			//PrintWriter writer = new PrintWriter(logFile);
			FileWriter fileWritter = new FileWriter(logFile,true);
	        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
	        PrintWriter writer = new PrintWriter(bufferWritter);
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rwd");
			FileChannel channel = randomAccessFile.getChannel();
			//FileLock lock = channel.lock();
			FileLock lock = channel.tryLock();
			if((lock!=null) && b)
			{
				 System.out.println(nodeNum + " Got a lock...");
				 System.out.println(nodeNum + " Performing file write for " + fileNo + " at time: " + System.currentTimeMillis());
				 long fileLength2 = file.length();
				 randomAccessFile.seek(fileLength2);
				 ByteBuffer bytes = ByteBuffer.allocate(MESSAGE_SIZE);
				 String msg1 = nodeNum + " writing into file \n";
				 bytes.put(msg1.getBytes());
				 bytes.flip();
				 channel.write(bytes);
				 channel.force(false);
				 long fileLength1 = file.length();
				 randomAccessFile.seek(fileLength1);
				 bytes.clear();
				 System.out.println(nodeNum + " executed write request no " + count + " from file: " + fileNo + " at time: " + System.currentTimeMillis());
				// System.out.println("Host " + nodeNum+ " is releasing the critical Section\n");
				// writer.println(nodeNum + " executed write request no " + count);
				 writer.println(nodeNum + " executed write request no " + count + " from file: " + fileNo + " at time: " + System.currentTimeMillis());
				 manager.releaseLock("write",fileNo);
			}
			else
			{
				writer.println("Error " + nodeNum + " could not execute write request no " + count + " from file: " + fileNo + " at time: " + System.currentTimeMillis());
				//writer.println("Error " + nodeNum + " could not execute write request no " + count);
				System.out.println("Error " + nodeNum + " could not execute write request no " + count);
			}
			lock.release();
			channel.close();
			writer.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}

}
