package project3;

import java.io.Serializable;

public class DataFile implements Serializable{
	
	int versionNum;
	int replicasUpdated;
	String distinguishedSite;
	boolean writeLock;
	int readLock;
	String fileName;
	
	public DataFile(String name, int noOfNodes)
	{
		writeLock = false;
		readLock = 0;
		fileName = name;
		distinguishedSite = null;
		versionNum = 0;
		replicasUpdated = noOfNodes;
	}
	
	public DataFile(int VN, int RU, String DS, boolean writeLock, int readLock)
	{
		versionNum = VN;
		replicasUpdated = RU;
		distinguishedSite =DS;
		writeLock = false;
		readLock = 0;
		
	}


}
