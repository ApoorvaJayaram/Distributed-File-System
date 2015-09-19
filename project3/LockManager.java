package project3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class LockManager {
	
	ArrayList<Integer> P = new ArrayList<Integer>();
	int M;
	ArrayList<Integer> Q = new ArrayList<Integer>();
	int N;
	String DS;
	
	HashMap<Integer,HostInfo> hosts = new HashMap<Integer,HostInfo>();
	int nodeNum;
	ConfigReader rd;
	ArrayList<Message> mm = new ArrayList<Message>();
	static boolean timeUp = false;
	Semaphore semaphore;
	Queue<Message> requestQueue = new LinkedList<Message>();
	ArrayList<Message> replyList = new ArrayList<Message>();
	CountDownLatch latch = new CountDownLatch(1);
	ServerSocket serverSocket;
	static int seqNo = 0;
	ExpoGen eGen = new ExpoGen();
	int mggFname;
	
	public LockManager(HashMap<Integer,HostInfo> h, int node, ConfigReader r, Semaphore sem)
	{
		hosts = h;
		nodeNum = node;
		rd = r;
		semaphore = sem;
		try {
			HostInfo hh = hosts.get(nodeNum);
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(hh.replyPort));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean sendRequest(String type, int fileName)
	{
		DataFile newFile = hosts.get(nodeNum).fileMap.get(fileName);
		int expValue = rd.minExp;
		while(!(expValue>rd.maxExp))
		{
			timeUp=false;
			System.out.println("Expvalue at node " + nodeNum + " is " + expValue);
			if(type.equalsIgnoreCase("read"))
			{
				System.out.println("inside the read of the Lock Manager "+nodeNum);
				while(true)
				{
					if(!newFile.writeLock)
					{
						//remove any other read request if present
						newFile.readLock++;
						System.out.println("got its own readLock "+nodeNum);
						seqNo = seqNo + 1;
						break;
						
					}
					else
					{
						seqNo = seqNo + 1;
						Message m = new Message(nodeNum,nodeNum,fileName,MsgType.READ_REQ,seqNo);
						System.out.println("adding read req inside Queue "+nodeNum);
						try {
							semaphore.acquire();
							requestQueue.add(m);
							semaphore.release();
							System.out.println("waiting to aquire its own lock "+nodeNum);
							System.out.println("read req latch count for node "+nodeNum+" is "+latch.getCount());
							latch.await();
							System.out.println("aquired its own lock "+nodeNum);
							latch = new CountDownLatch(1);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
			}
			else
				if(type.equalsIgnoreCase("write"))
				{
					System.out.println("inside the write lock "+nodeNum);
					while(true)
					{
						if(!newFile.writeLock && !(newFile.readLock>0))
						{
							newFile.writeLock = true;
							System.out.println("got write lock "+nodeNum);
							seqNo = seqNo + 1;
							break;
							
						}
						else
						{
							seqNo = seqNo + 1;
							Message m = new Message(nodeNum,nodeNum,fileName,MsgType.WRITE_REQ,seqNo);
							try {
								semaphore.acquire();
								requestQueue.add(m);
								System.out.println("adding write req inside Queue "+nodeNum);
								semaphore.release();
								System.out.println("waiting to aquire its own lock "+nodeNum);
								System.out.println("write req latch count for node "+nodeNum+" is "+latch.getCount());
								latch.await();
								System.out.println("aquired its own lock "+nodeNum);
								latch = new CountDownLatch(1);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					
				}
			System.out.println("spawning a new thread for the node "+nodeNum);
					new Thread(){
						
						public void run()
						{
							HostInfo hh = hosts.get(nodeNum);
							Message msg;
							//ServerSocket serverSocket;
							try
							{
								
								while(!timeUp)
								{
										
											System.out.println(nodeNum + "I am stuck at accept()");
											//Listens for a connection to be made to this socket and accepts it
											//The method blocks until a connection is made
											Socket sockk = serverSocket.accept();
											System.out.println(nodeNum + "done accept()");
											OutputStream os = sockk.getOutputStream();
										    ObjectOutputStream oos = new ObjectOutputStream(os);
										    InputStream is = sockk.getInputStream();
										    ObjectInputStream ois = new ObjectInputStream(is);
										    
										    msg = (Message)ois.readObject();
										    System.out.println("node "+nodeNum+" got reply from "+msg.sender);
										    System.out.println("Current seq no: " +seqNo + " msg seq no: " + msg.seqNo);
										    
										    if((msg.type == MsgType.REPLY) && (msg.seqNo == seqNo))
										    {
										    	System.out.println("node "+nodeNum+" got reply from "+msg.sender);
										    	System.out.println("Got semaphore for add in P " + nodeNum);
										    	if(!timeUp)
										    	{
										    		//semaphore.acquire();
											    	mm.add(msg);
											    	P.add(msg.sender);
											    	//semaphore.release();
										    	}
										    	else
										    		break;
										    	System.out.println("node "+nodeNum+" added reply from "+msg.sender + " in the queue");
										    }
										
								}
								System.out.println("Breaking the thread while loop at " + nodeNum);
							//serverSocket.close();
							}
							catch(Exception e)
							{
								e.printStackTrace();
							}
							
						}
						
					}.start();
					
					
			if(type.equalsIgnoreCase("read"))
			{
				System.out.println("sending req to all nodes "+nodeNum+" for the read");
				for(int h : hosts.keySet())
				{
					HostInfo host = hosts.get(h);
					if(host.nodeNumber!=nodeNum)
					{
						//seqNo = seqNo + 1;
						Message msg = new Message(nodeNum,host.nodeNumber,fileName,MsgType.READ_REQ,seqNo);
						//try {
							System.out.println("sender: "+msg.sender+"--->"+"receiver: "+msg.receiver);
							//semaphore.acquire();
							sendMessage(msg,false);
							//semaphore.release();
						//} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						//}
					}
						
				}
			}
			else
				if(type.equalsIgnoreCase("write"))
				{
					System.out.println("sending req to all nodes "+nodeNum+" for the write");
					for(int h : hosts.keySet())
					{
						HostInfo host = hosts.get(h);
						if(host.nodeNumber!=nodeNum)
						{
							//seqNo = seqNo + 1;
							Message msg = new Message(nodeNum,host.nodeNumber,fileName,MsgType.WRITE_REQ,seqNo);
							//try {
								System.out.println("sender: "+msg.sender+"--->receiver: "+msg.receiver);
								//semaphore.acquire();
								sendMessage(msg,false);
								//semaphore.release();
							//} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								//e.printStackTrace();
							//}
						}
							
					}
				}
					
			try {
				Thread.sleep(expValue);
				timeUp=true;
				
				//adding itself to the partition and Quorum on obtaining its own lock
				//semaphore.acquire();
	            P.add(nodeNum);
	            //semaphore.release();
	            
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            //Q.add(nodeNum);
            Message selfMsg = new Message(nodeNum,nodeNum,newFile,MsgType.REPLY,fileName);
            System.out.println("node "+nodeNum+" adding itself to the partition and Quorum on obtaining its own lock for read/write operation");
            mm.add(selfMsg);
            //ends
			//Logic to check condition of Q>N/2 or Q=N/2 and DS E Q
			
			int vno = 0;
			for(int i=0;i<mm.size();i++)
			{
				DataFile f = mm.get(i).file;
				if(f.versionNum>vno)
				{
					vno = f.versionNum;
				}
			}
			M = vno;
			
			for(int j =0;j<mm.size();j++)
			{
				DataFile f = mm.get(j).file;
				if(f.versionNum == M)
				{
					Q.add(mm.get(j).sender);
					N = f.replicasUpdated;
					DS = f.distinguishedSite;
				}
			}
			System.out.println("Quorum size::: "+Q.size()+" node: "+nodeNum+" DS: "+DS+" N: "+N+" P size: "+P.size()+" Max version No. "+M);
			if(Q.size() > N/2)
			{
				//send ack to all sites except the ones in the Q
				sendACK(type,fileName);
				//if my version old send request content msg if write operation
				HostInfo hme = hosts.get(nodeNum);
				if(hme.fileMap.get(fileName).versionNum<M)
				{
					int nodeForContent = Q.get(0);
					Message msgContent = new Message(nodeNum,nodeForContent,MsgType.REQUEST_CONTENT,fileName);
					//try {
						System.out.println("node "+msgContent.sender+" requesting for the content from node "+msgContent.receiver);
						//semaphore.acquire();
						sendMessage(msgContent,false);
						//semaphore.release();
					//} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
					//}
					
				}
				System.out.println("Got quorum for " + type + " returning true by node " + nodeNum);
				expValue=rd.minExp;
				return true;
			}
			else
			{
				if(DS!=null)
				{
					System.out.println("checking Q=N/2 condition for node "+nodeNum);
					if((Q.size()==N/2) && (Q.contains(Integer.parseInt(DS))))
					{
						System.out.println("satisfied Q=N/2 condition for node "+nodeNum);
						//send ack
						sendACK(type,fileName);
						//if my version old send request content msg if write operation
						HostInfo hme = hosts.get(nodeNum);
						if(hme.fileMap.get(fileName).versionNum<M)
						{
							int nodeForContent = Q.get(0);
							Message msgContent = new Message(nodeNum,nodeForContent,MsgType.REQUEST_CONTENT,fileName);
							//try {
								System.out.println("node "+msgContent.sender+" requesting for the content from node "+msgContent.receiver);
								//semaphore.acquire();
								sendMessage(msgContent,false);
								//semaphore.release();
							//} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								//e.printStackTrace();
							//}
						}
						System.out.println("Got quorum for " + type + " returning true by node " + nodeNum);
						expValue=rd.minExp;
						return true;
					}
				}
			}
			//send abort
			releaseOwnLock(type,fileName);
			sendAbort(type,fileName);
			resetValues();
			expValue = expValue + 100;
			System.out.println("Expvalue at node " + nodeNum + " is " + expValue);
			Random rand = new Random();
			long val = rand.nextInt((100 - 1) + 1) + 1;
			try {
				Thread.sleep(val);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//send abort
		//sendAbort(type,fileName);
		//releaseOwnLock(type,fileName);
		//resetValues();
		System.out.println("Returning false for operation " + type + " at node " + nodeNum);
		expValue=rd.minExp;
		return false;
	}
	
	public void releaseOwnLock(String type,int fileName)
	{
		if(type.equalsIgnoreCase("read"))
		{
			DataFile nFile = hosts.get(nodeNum).fileMap.get(fileName);
			nFile.readLock--;
			System.out.println("After abort released my own read lock " + nodeNum);
		}
		else
		{
			if(type.equalsIgnoreCase("write"))
			{
				DataFile nFile = hosts.get(nodeNum).fileMap.get(fileName);
				nFile.writeLock = false;
				System.out.println("After abort released my own write lock " + nodeNum);
			}
		}
	}
	
	public void resetValues()
	{
		System.out.println("Resetting values for P,Q,M,N "+nodeNum);
		P.clear();
		Q.clear();
		//ArrayList<Integer> P = new ArrayList<Integer>();
		//ArrayList<Integer> Q = new ArrayList<Integer>();
		mm.clear();
		M=0;
		N=0;
		//DS="";
	}
	public void sendACK(String type, int fNum) {
		
		Message m;
		for (int i = 0; i < rd.noOfNodes; i++) {
			if (!P.contains(i) && (i!=nodeNum)) {
				System.out.println("sending ACK to node "+i +" from "+nodeNum);
				if (type.equalsIgnoreCase("read"))
					m = new Message(nodeNum, i, MsgType.ACK, fNum,
							MsgType.READ_REQ);
				else
					m = new Message(nodeNum, i, MsgType.ACK, fNum,
							MsgType.WRITE_REQ);
				sendMessage(m, false);
			}
		}

	}
	
	public void sendAbort(String type, int fileName)
	{
		//to all sites
		//HostInfo host = hosts.get(nodeNum);
		MsgType msgType;
		if(type.equalsIgnoreCase("read"))
		{
			msgType = MsgType.READ_REQ;
		}
		else
		{
			msgType = MsgType.WRITE_REQ;
		}
		for(int h : hosts.keySet())
		{
			HostInfo host = hosts.get(h);
			if(host.nodeNumber!=nodeNum)
			{
				Message msg = new Message(nodeNum,host.nodeNumber,MsgType.ACK,fileName,msgType);
				//try {
					System.out.println("sending abort to node "+h +" from "+nodeNum);
					//semaphore.acquire();
					sendMessage(msg,false);
					//semaphore.release();
				//} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				//}
			}
		}
		//processOtherReq(type,fileName);
	}
	
	
	public void processOtherReq(String type,int fName)
	{
		System.out.println("After sending abort processing request from queue " + nodeNum);
		System.out.println("inside processOtherReq for node "+nodeNum);
		Iterator<Message> itr = requestQueue.iterator();
		
		if(type.equalsIgnoreCase("read"))
		{
			if(hosts.get(nodeNum).fileMap.get(fName).readLock == 0 && !hosts.get(nodeNum).fileMap.get(fName).writeLock)
			{
				//if(!requestQueue.isEmpty())
				
				while(itr.hasNext())
				{
					Message mr = itr.next();
					if(mr.fName == fName)
					{
						try {
							semaphore.acquire();
							//requestQueue.remove(mr);
							itr.remove();
							semaphore.release();
							System.out.println("processing req from the queue from "+mr.sender+" type:: "+mr.type);
							if((mr.sender==nodeNum) && (mr.receiver==nodeNum))
							{
								System.out.println("own request "+nodeNum);
								latch.countDown();
							}
							else
								processRequest(mr);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						break;
					}
				}
			}
		}
		else 
			if(type.equalsIgnoreCase("write"))
			{
				try
				{
					if(hosts.get(nodeNum).fileMap.get(fName).readLock == 0 && !hosts.get(nodeNum).fileMap.get(fName).writeLock)
					{
						//if(!requestQueue.isEmpty())
						while(itr.hasNext())
						{
							Message mr = itr.next();
							if(mr.fName == fName)
							{
									semaphore.acquire();
									//requestQueue.remove(mr);
									itr.remove();
									semaphore.release();
									System.out.println("processing req from the queue from "+mr.sender+" type:: "+mr.type);
									if((mr.sender==nodeNum) && (mr.receiver==nodeNum))
									{
										System.out.println("own req "+nodeNum);
										latch.countDown();
									}
									else
										processRequest(mr);
									
								int fileName = mr.fName;
								if(mr.type == MsgType.READ_REQ)
								{
									Iterator<Message> iterator = requestQueue.iterator();
									while(iterator.hasNext())
									{
										Message mg = iterator.next();
										if((mg.type == MsgType.READ_REQ) && (mg.fName == fileName))
										{
											System.out.println("node "+nodeNum+" taking out the read reqs from the req queue "+mg.sender);
											semaphore.acquire();
											//requestQueue.remove(mg);
											iterator.remove();
											semaphore.release();
											processRequest(mg);
										}
									}
								}
								break;
							}
						}
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			
			}
	}
	
	public void processRequest(Message m)
	{
		DataFile f = hosts.get(nodeNum).fileMap.get(m.fName);
		if(m.type == MsgType.READ_REQ)
		{
			if(!f.writeLock)
			{
				Message rMsg = new Message(nodeNum,m.sender,f,MsgType.REPLY,m.seqNo);
				f.readLock++;
				System.out.println("process req read aquired by "+nodeNum+" from "+m.sender);
				try {
					semaphore.acquire();
					replyList.add(m);
					if(requestQueue.contains(m))
						requestQueue.remove(m);
					semaphore.release();
					//Thread.sleep(1000);
					System.out.println("sending reply for read req from"+nodeNum+" to "+m.sender);
					sendMessage(rMsg,true);
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				try {
					System.out.println("putting inside the req que for read by"+nodeNum+" request of "+m.sender);
					semaphore.acquire();
					requestQueue.add(m);
					semaphore.release();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else
			if(m.type == MsgType.WRITE_REQ)
			{
				if(!f.writeLock && !(f.readLock>0))
				{
					Message rMsg = new Message(nodeNum,m.sender,f,MsgType.REPLY,m.seqNo);
					f.writeLock = true;
					System.out.println("process req write aquired by "+nodeNum+" from "+m.sender);
					try {
						semaphore.acquire();
						replyList.add(m);
						if(requestQueue.contains(m))
							requestQueue.remove(m);
						//Thread.sleep(1000);
						semaphore.release();
						System.out.println("sending reply for write req from"+nodeNum+" to "+m.sender);
						sendMessage(rMsg,true);
						
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else
				{
					try {
						System.out.println("putting inside the req que for write by "+nodeNum+" request of "+m.sender);
						semaphore.acquire();
						requestQueue.add(m);
						semaphore.release();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
	}
	
	public void processMessage(Message m)
	{
		if(m.type == MsgType.ACK)
		{
			System.out.println("processing ACK msg "+nodeNum+" sender: "+m.sender);
			//different handling if sender in queue and sender not in queue
			//not in queue ACK may be for reply sent for read/write request reached after timer gone off
			boolean b = false;
			try {
				System.out.println("Waiting for lock at ack 1 " + nodeNum);
				semaphore.acquire();
				System.out.println("Got lock at ack " + nodeNum);
				Iterator<Message> itf = requestQueue.iterator();
				while(itf.hasNext())
				{
					Message mp = itf.next();
					if((mp.sender == m.sender) && (mp.type == m.initialRequestType) && (mp.fName == m.fName))
					{
						//semaphore.acquire();
						//requestQueue.remove(mp);
						itf.remove();
						//semaphore.release();
						System.out.println("removed from the ack req Queue "+nodeNum+" sender: "+m.sender);
						b=true;
					}
				}
				semaphore.release();
				if(!b)
				{
					System.out.println("Waiting for lock at ack 2 " + nodeNum);
					semaphore.acquire();
					System.out.println("Got lock at ack " + nodeNum);
					Iterator<Message> itr = replyList.iterator();
					while(itr.hasNext())
					{
							Message md = itr.next();
							if((md.sender == m.sender) && (md.type == m.initialRequestType) && (md.fName == m.fName))
							{
								if(md.type == MsgType.READ_REQ)
									hosts.get(nodeNum).fileMap.get(m.fName).readLock = hosts.get(nodeNum).fileMap.get(m.fName).readLock - 1;
								else
									if(md.type == MsgType.WRITE_REQ)
										hosts.get(nodeNum).fileMap.get(m.fName).writeLock = false;
								System.out.println("removing from the ack replylist "+nodeNum+" sender: "+m.sender);
								//semaphore.acquire();
								itr.remove();
								//semaphore.release();
								System.out.println("removed from the ack replylist "+nodeNum+" sender: "+m.sender);
								
							}
					}
					semaphore.release();
					
					Message mgg=null;
					boolean present = false;
					Iterator<Message> iter = requestQueue.iterator();
					if(hosts.get(nodeNum).fileMap.get(m.fName).readLock == 0 && !hosts.get(nodeNum).fileMap.get(m.fName).writeLock)
					{
						//if(!requestQueue.isEmpty())
						while(iter.hasNext())
						{
							mgg = iter.next();
							if(mgg.fName == m.fName)
							{
								present = true;
								//requestQueue.remove(mgg);
								semaphore.acquire();
								iter.remove();
								semaphore.release();
								System.out.println("processing req from the queue from "+mgg.sender+" type:: "+mgg.type);
								if((mgg.sender==nodeNum) && (mgg.receiver==nodeNum))
								{
									System.out.println("own request "+nodeNum);
									latch.countDown();
								}
								else
									processRequest(mgg);
								break;
							}
						}
					}
					
					if(present == true)
					{
						mggFname = mgg.fName;
						if((mgg!=null) && (mgg.type == MsgType.READ_REQ))
						{
							new Thread()
							{
								public void run()
								{
									Iterator<Message> iterator = requestQueue.iterator();
									System.out.println("Waiting get lock for processing more read requests in the queue " + nodeNum);
									try {
										
										System.out.println("Got lock..");
										while(iterator.hasNext())
										{
											Message mg = iterator.next();
											if((mg.type == MsgType.READ_REQ) && (mg.fName == mggFname))
											{
												System.out.println("node "+nodeNum+" taking out the read reqs from the req queue "+mg.sender);
												semaphore.acquire();
												iterator.remove();
												semaphore.release();
												processRequest(mg);
											}
										}
										//semaphore.release();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}.start();
						}
					}
				}
				//semaphore.release();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(m.type == MsgType.RELEASE_READ_LOCK)
		{
			System.out.println("processing release read lock msg "+nodeNum+" sender: "+m.sender);
			try
			{
				semaphore.acquire();
				Iterator<Message> iter = replyList.iterator();
				while(iter.hasNext())
				{
					Message mg = iter.next();
					if((mg.sender == m.sender) && (mg.fName == m.fName))
					{
						System.out.println("removing from the release read lock replylist "+nodeNum+" sender: "+m.sender);
						//semaphore.acquire();
						iter.remove();
						//semaphore.release();
					}
				}
				semaphore.release();
				
				Iterator<Message> itt = requestQueue.iterator();
				hosts.get(nodeNum).fileMap.get(m.fName).readLock = hosts.get(nodeNum).fileMap.get(m.fName).readLock - 1;
				if(hosts.get(nodeNum).fileMap.get(m.fName).readLock == 0 && !hosts.get(nodeNum).fileMap.get(m.fName).writeLock)
				{
					//if(!requestQueue.isEmpty())
					while(itt.hasNext())
					{
						Message mt = itt.next();
						if(mt.fName == m.fName)
						{
							semaphore.acquire();
							//requestQueue.remove(mt);
							itt.remove();
							semaphore.release();
							System.out.println("processing req from the queue from "+mt.sender+" type:: "+mt.type);
							if((mt.sender==nodeNum) && (mt.receiver==nodeNum))
							{
								System.out.println("own request "+nodeNum);
								latch.countDown();
							}
							else
								processRequest(mt);
							break;
						}
					}
				}
				
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(m.type == MsgType.RELEASE_WRITE_LOCK)
		{
			System.out.println("processing release write lock msg "+nodeNum+" sender: "+m.sender);
			DataFile ff = m.file;
			int fNam = Integer.parseInt(ff.fileName);
			//DataFile fMy = hosts.get(nodeNum).fileMap.get(fNam);
			//update version no, RU and DS
			hosts.get(nodeNum).fileMap.get(fNam).replicasUpdated = ff.replicasUpdated;
			hosts.get(nodeNum).fileMap.get(fNam).versionNum = ff.versionNum;
			hosts.get(nodeNum).fileMap.get(fNam).distinguishedSite = ff.distinguishedSite;
			HostInfo hh = hosts.get(nodeNum);
			String fileName = "./root/" + hh.hostName + "/" + fNam + ".txt";
			PrintWriter writer;
			try {
				System.out.println("reply content from "+m.receiver+" sent by "+m.sender);
				writer = new PrintWriter(fileName);
				writer.print("");
				writer.print(m.content);
				writer.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try
			{
				semaphore.acquire();
				Iterator<Message> itr = replyList.iterator();
				while(itr.hasNext())
				{
					Message md = itr.next();
					if((md.sender == m.sender) && (md.fName == fNam))
					{
						System.out.println("removing from the release write lock replylist "+nodeNum+" sender: "+m.sender);
						//semaphore.acquire();
						itr.remove();
						//semaphore.release();
					}
				}
				semaphore.release();
				hosts.get(nodeNum).fileMap.get(fNam).writeLock = false;
				Iterator<Message> itm = requestQueue.iterator();
				if(hosts.get(nodeNum).fileMap.get(fNam).readLock == 0 && !hosts.get(nodeNum).fileMap.get(fNam).writeLock)
				{
					//if(!requestQueue.isEmpty())
					while(itm.hasNext())
					{
						Message mtg = itm.next();
						if(mtg.fName == fNam)
						{
							semaphore.acquire();
							//requestQueue.remove(mtg);
							itm.remove();
							semaphore.release();
							System.out.println("processing req from the queue from "+mtg.sender+" type:: "+mtg.type);
							if((mtg.sender==nodeNum) && (mtg.receiver==nodeNum))
							{
								System.out.println("own req "+nodeNum);
								latch.countDown();
							}
							else
								processRequest(mtg);
							//int ffName = mtg.fName;
							if(mtg.type == MsgType.READ_REQ)
							{
								//semaphore.acquire();
								Iterator<Message> iterator = requestQueue.iterator();
								while(iterator.hasNext())
								{
									Message mg = iterator.next();
									if((mg.type == MsgType.READ_REQ) && (mg.fName == fNam))
									{
										System.out.println("node "+nodeNum+" taking out the read reqs from the req queue "+mg.sender);
										semaphore.acquire();
										//requestQueue.remove(mg);
										iterator.remove();
										semaphore.release();
										processRequest(mg);
									}
								}
								//semaphore.release();
							}
							break;
						}
					}
				}
				
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(m.type == MsgType.REQUEST_CONTENT)
		{
			String data = fetchContent(m.fName);
			try
			{
			    Message rMsg = new Message(nodeNum,m.sender,MsgType.REPLY_CONTENT,data,m.fName);
			    System.out.println("req content from "+rMsg.receiver+" sent by "+rMsg.sender);
			    //semaphore.acquire();
			    sendMessage(rMsg,false);
			    //semaphore.release();
			    
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		if(m.type == MsgType.REPLY_CONTENT)
		{
			HostInfo hh = hosts.get(nodeNum);
			DataFile f = hh.fileMap.get(m.fName);
			String fileName = "./root/" + hh.hostName + "/" + f.fileName + ".txt";
			PrintWriter writer;
			try {
				System.out.println("reply content from "+m.receiver+" sent by "+m.sender);
				writer = new PrintWriter(fileName);
				writer.print("");
				writer.print(m.content);
				writer.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void releaseLock(String type, int fileNo)
	{
		
		Message rMsg;
		String content;
		DataFile f = hosts.get(nodeNum).fileMap.get(fileNo);
		if(type.equalsIgnoreCase("write"))
		{
			hosts.get(nodeNum).fileMap.get(fileNo).versionNum = hosts.get(nodeNum).fileMap.get(fileNo).versionNum + 1;
			hosts.get(nodeNum).fileMap.get(fileNo).replicasUpdated = P.size();
			if((P.size()!=0) && (P.size()%2) == 0)
			{
				System.out.println(" P size is even " + P.size() + " at node "+nodeNum);
				hosts.get(nodeNum).fileMap.get(fileNo).distinguishedSite = Integer.toString(P.get(0));
				//send message
				// send the read lock release message to all the nodes in the
				// partition
				hosts.get(nodeNum).fileMap.get(fileNo).writeLock = false;
			}
			else
			{
				System.out.println("P size is odd " + P.size() + " at node "+nodeNum);
				//send update to all nodes in P
				hosts.get(nodeNum).fileMap.get(fileNo).distinguishedSite = null;
				hosts.get(nodeNum).fileMap.get(fileNo).writeLock = false;
			}
			System.out.println("fetching the filecontent "+nodeNum+" fileNo "+fileNo);
			content = fetchContent(fileNo);
			for (int i = 0; i < mm.size(); i++) {
				if(mm.get(i).receiver != mm.get(i).sender)
                {
					rMsg = new Message(nodeNum, mm.get(i).sender, MsgType.RELEASE_WRITE_LOCK, f, content,mm.get(i).type);
					System.out.println("sending the file content and release write from "+rMsg.sender+" to "+rMsg.receiver);
					//try {
						System.out.println("Waiting to acquire lock for sendMsg at release " + nodeNum);
						//semaphore.acquire();
						System.out.println(" sent release " + nodeNum);
						sendMessage(rMsg, false);
						//semaphore.release();
					//} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
					//}
                }
			}
			//handle the requests in the queue
			try
			{
				Iterator<Message> itd = requestQueue.iterator();
				//semaphore.acquire();
				if(hosts.get(nodeNum).fileMap.get(fileNo).readLock == 0 && !hosts.get(nodeNum).fileMap.get(fileNo).writeLock)
				{
					//if(!requestQueue.isEmpty())
					//semaphore.acquire();
					System.out.println("After releasing own write lock handling requests inside request queue " + nodeNum);
					while(itd.hasNext())
					{
						Message mdh = itd.next();
						if(mdh.fName == fileNo)
						{
							//requestQueue.remove(mdh);
							semaphore.acquire();
							itd.remove();
							semaphore.release();
							if((mdh.sender==nodeNum) && (mdh.receiver==nodeNum))
							{
								System.out.println("My own request nodeno " +nodeNum + " request type " +mdh.type);
								latch.countDown();
							}
							else
								processRequest(mdh);
							System.out.println("Processed request inside request queue by " +nodeNum + " request made by&type " + mdh.sender + mdh.type);
							if(mdh.type == MsgType.READ_REQ)
							{
								Iterator<Message> iterator = requestQueue.iterator();
								while(iterator.hasNext())
								{
									Message mg = iterator.next();
									if((mg.type == MsgType.READ_REQ) && (mg.fName == fileNo))
									{
										System.out.println("handling the read req after release lock from "+mg.sender+" to "+mg.receiver);
										semaphore.acquire();
										//requestQueue.remove(mg);
										iterator.remove();
										semaphore.release();
										processRequest(mg);
									}
								}
							}
							break;
						}
					}
				}
				//semaphore.release();
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//if the operation is read
		if(type.equalsIgnoreCase("read"))
		{
			System.out.println(" release lock Operation is read " + nodeNum);
			try
			{
				//semaphore.acquire();
				hosts.get(nodeNum).fileMap.get(fileNo).readLock = hosts.get(nodeNum).fileMap.get(fileNo).readLock - 1;
				System.out.println("Released read lock " +nodeNum);
				for (int i = 0; i < mm.size(); i++) {
					if(mm.get(i).receiver != mm.get(i).sender)
	                {
						rMsg = new Message(nodeNum, mm.get(i).sender, MsgType.RELEASE_READ_LOCK,mm.get(i).type, fileNo);
						System.out.println("sending release read from "+rMsg.sender+" to "+rMsg.receiver);
						//try {
							System.out.println("Waiting to acquire lock for sendMsg at release " + nodeNum);
							//semaphore.acquire();
							System.out.println(" sent release " + nodeNum);
							sendMessage(rMsg, false);
							//semaphore.release();
						//} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						//}
	                }
				}
				Iterator<Message> iit = requestQueue.iterator();
				if(hosts.get(nodeNum).fileMap.get(fileNo).readLock == 0 && !hosts.get(nodeNum).fileMap.get(fileNo).writeLock)
				{
					System.out.println("After releasing own write lock handling requests inside request queue " + nodeNum);
					//if(!requestQueue.isEmpty())
					while(iit.hasNext())
					{
						Message mlr = iit.next();
						if(mlr.fName == fileNo)
						{
							System.out.println("Release read lock request queue not empty " +nodeNum);
							semaphore.acquire();
							//requestQueue.remove(mlr);
							iit.remove();
							semaphore.release();
							System.out.println("Release read lock from request queue processing request to  " +nodeNum + "from " + mlr.sender);
							if((mlr.sender==nodeNum) && (mlr.receiver==nodeNum))
							{
								System.out.println("My own request.. countdown");
								latch.countDown();
							}
							else
								processRequest(mlr);
							System.out.println("Processed request inside request queue by " +nodeNum + " request made by&type " + mlr.sender + mlr.type);
							break;
						}
					}
				}
				//semaphore.release();
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//reset P,Q,M,N
		resetValues();
	}
	
	
	public void sendMessage(Message rMsg, boolean flag)
	{
		System.out.println("The thread is " + Thread.currentThread().getName());
		HostInfo h = hosts.get(rMsg.receiver);
		Socket clientSocket = null;
		String hname = h.hostName + ".utdallas.edu";
		if(flag == false)
			System.out.println( nodeNum + " Connecting to node " + h.nodeNumber + "at port: " + h.requestPort + " " + rMsg.type);
		else
			System.out.println( nodeNum + " Connecting to node " + h.nodeNumber + "at port: " + h.replyPort + " " + rMsg.type);
		
		try
		{
			//Create a client socket and connect to server at 127.0.0.1 port 5000
			if(flag == false)
				clientSocket = new Socket(hname,h.requestPort);
			else
				clientSocket = new Socket(hname,h.replyPort);
			
			//Read messages from server. Input stream are in bytes. They are converted to characters by InputStreamReader
			//Characters from the InputStreamReader are converted to buffered characters by BufferedReader
			 OutputStream os = clientSocket.getOutputStream();
		     ObjectOutputStream oos = new ObjectOutputStream(os);
		     //InputStream is = clientSocket.getInputStream();
		     //ObjectInputStream ois = new ObjectInputStream(is);
		     oos.writeObject(rMsg);
		     oos.flush();
		     System.out.println(nodeNum + "Message sent to " +  h.nodeNumber);
		     System.out.println(" the status of of the client socket is "+ clientSocket.isConnected());
		    
		     clientSocket.shutdownOutput();
		     
		     oos.close();
		     os.close();
		     System.out.println(" the status of of the client socket is "+ clientSocket.isConnected());
		     //clientSocket.shutdownOutput();
		     //clientSocket.close();
		}
		catch(Exception ex)
		{
			
			
			System.out.println("There is an exception in application at " + nodeNum);
			System.out.println("The message from " + rMsg.sender + " to " + rMsg.receiver + " msg type " + rMsg.type);
			ex.printStackTrace();
		}
		
	}
	
	public String fetchContent(int fileNo) {
		System.out.println("Fetching content from " + fileNo);
		String name = rd.hosts.get(nodeNum).hostName;
		String pathname = "./root/" + name + "/" + Integer.toString(fileNo) +  ".txt";
		File file = new File(pathname);
		StringBuilder fileContents = new StringBuilder((int) file.length());
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String lineSeparator = System.getProperty("line.separator");

		try {
			while (scanner.hasNextLine()) {
				fileContents.append(scanner.nextLine() + lineSeparator);
			}
			return fileContents.toString();
		} finally {
			scanner.close();
		}

	}
}
