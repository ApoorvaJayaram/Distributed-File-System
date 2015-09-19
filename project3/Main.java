package project3;

import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.io.File;
import java.io.IOException;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int nodeNum = Integer.parseInt(args[0].trim());
		//int nodeNum = 5;
		//String fName = "OutforNode" + nodeNum;
		//File f = new File("./" + fName);
		ConfigReader rd = new ConfigReader(nodeNum);
		System.out.println("The read file");
		rd.readFile("Configuration.txt");
		System.out.println("No of nodes: " + rd.noOfNodes);
		System.out.println("No of files: " + rd.noOfFiles);
		System.out.println("No of operations: " + rd.noOfOperations);
		System.out.println("Delay: " + rd.meanDelay);
		System.out.println("No of read: " + rd.noOfRead);
		System.out.println("No of write: " + rd.noOfWrite);
		System.out.println("Expo min: " + rd.minExp);
		System.out.println("Expo max: " + rd.maxExp);
		for(Integer host : rd.hosts.keySet())
		{
			System.out.println("Host number is: " + host);
			HostInfo h = rd.hosts.get(host);
			System.out.println("Host name is: " + h.hostName);
			System.out.println("Rquest Port is: " + h.requestPort);
			System.out.println("Reply Port is: " + h.replyPort);
		}
		
		Main m = new Main();
		m.createDirectory(rd, nodeNum, rd);
		
		Semaphore semaphore=new Semaphore(1);
		
		LockManager manager = new LockManager(rd.hosts,nodeNum,rd,semaphore);
		
		Server ser = new Server(rd.hosts,nodeNum,rd,semaphore,manager);
		ser.start();
		
		try
		{
			Thread.sleep(15000);
		}
		catch(InterruptedException e) {}
		
		Node node = new Node(nodeNum,rd,semaphore,manager);
		node.execute();

	}
	
	public void createDirectory(ConfigReader r, int nodeNum, ConfigReader rd)
	{
		HostInfo h = r.hosts.get(nodeNum);
		File file = new File("./root/" + h.hostName);
		if (!file.exists()) {
			if (file.mkdir()) {
				System.out.println("Directory is created!");
			} else {
				System.out.println("Failed to create directory!");
			}
		}
		
		for(int i=1;i<=r.noOfFiles;i++)
		{
			try {
				 
			      File f = new File("./root/" + h.hostName + "/" + i + ".txt");
		 
			      if (!f.createNewFile()){
			        System.out.println("File already exists.");
			      }
			      
			      DataFile df = new DataFile(Integer.toString(i),rd.noOfNodes);
			      h.fileMap.put(i, df);
		 
		    	} catch (IOException e) {
			      e.printStackTrace();
		    	}
		}
		
		for(Integer ff : r.hosts.get(nodeNum).fileMap.keySet())
		{
			DataFile d = r.hosts.get(nodeNum).fileMap.get(ff);
			System.out.println("Name: " + d.fileName);
		}
		
	}

}
