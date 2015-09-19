package project3_Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class HostInfo {
	
	int nodeNumber;
	String hostName;
	int requestPort;
	int replyPort;
	HashMap<Integer,DataFile> fileMap = new HashMap<Integer,DataFile>();
	
	/*public HostInfo()
	{
		ourSeqNo = 0;
		hgstSeqNo = 0;
		using = false;
		waiting = false;
	}
	
	public HostInfo(int n)
	{
		ourSeqNo = 0;
		hgstSeqNo = 0;
		using = false;
		waiting = false;
		A = new boolean[n];
		replyDeferred = new boolean[n];
		Arrays.fill(A, Boolean.FALSE);
		Arrays.fill(replyDeferred, Boolean.FALSE);
	}*/
}
