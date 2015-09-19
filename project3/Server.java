package project3;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

//receives the reply from the other nodes
public class Server implements Runnable{
	
	HashMap<Integer,HostInfo> hosts = new HashMap<Integer,HostInfo>();
	int nodeNum;
	ConfigReader rd;
	Semaphore semaphore;
	Thread t;
	final int MESSAGE_SIZE = 50000;
	LockManager manager;
	Message msg;
	
	public Server(HashMap<Integer,HostInfo> hst, int num, ConfigReader r, Semaphore sem, LockManager mgr)
	{
		hosts = hst;
		nodeNum = num;
		rd = r;
		semaphore = sem;
		manager = mgr;
	}

	public void run()
	{
		HostInfo hh = hosts.get(nodeNum);
		
		try
		{
			//Create a server socket at port 5000
			ServerSocket serverSock = new ServerSocket(hh.requestPort);
			//serverSock.setSoTimeout(80000);
			System.out.println("My time out time " + serverSock.getSoTimeout() + " at node " + nodeNum);
			//Server goes into a permanent loop accepting connections from clients		
			while(true)
			{
				
				System.out.println(nodeNum + "I am stuck at accept()");
				//Listens for a connection to be made to this socket and accepts it
				//The method blocks until a connection is made
				Socket sock = serverSock.accept();
				sock.setReuseAddress(true);
				System.out.println(nodeNum + "done accept()");
				OutputStream os = sock.getOutputStream();
			    ObjectOutputStream oos = new ObjectOutputStream(os);
			    InputStream is = sock.getInputStream();
			    ObjectInputStream ois = new ObjectInputStream(is);
			    
			    msg = (Message)ois.readObject();
			   
			    
			    if(msg.type == MsgType.READ_REQ || (msg.type == MsgType.WRITE_REQ))
			    {
			    	System.out.println(nodeNum + "Accepted msg from " + msg.sender + " service sending for processing");
			    	manager.processRequest(msg);
			    }
			    else
			    {
			    	System.out.println(nodeNum + "Accepted msg from " + msg.sender + " service sending for processing");
			    	manager.processMessage(msg);
			    }
			    //oos.flush();
			    ois.close();
		        oos.close();
			}
		}
		catch(Exception ex)
		{
			
			//System.out.println("There is an exception at " + nodeNum + " my time out is " + serverSock.);
			System.out.println("The message from " + msg.sender + " to " + msg.receiver);
			ex.printStackTrace();
		}
	
	}
	
	public void start ()
	{
	    //  System.out.println("Starting Server " +  threadName );
	      if (t == null)
	      {
	         t = new Thread (this, "Servicehost " + nodeNum);
	         t.start ();
	      }
	}
	
	public byte[] toByteArray(ByteBuffer byteBuffer)
	{
		byteBuffer.position(0);
		byteBuffer.limit(MESSAGE_SIZE);
		byte[] bufArr = new byte[byteBuffer.remaining()];
		byteBuffer.get(bufArr);
		return bufArr;
	}
	
}
