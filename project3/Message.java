package project3;

import java.io.Serializable;

public class Message implements Serializable {

	int sender;
	int receiver;
	DataFile file;
	MsgType type;
	String content;
	int fName;
	MsgType initialRequestType;
	int seqNo;
	
//for req msgs 
	public Message(int senderID, int receiverID, int name, MsgType mType, int no) {
		this.sender = senderID;
		this.receiver = receiverID;
		fName = name;
		type = mType;
		seqNo = no;
	}
	
//for reply msgs	
	public Message(int senderID, int receiverID, DataFile f, MsgType mType, int no) {
		this.sender = senderID;
		this.receiver = receiverID;
		file = f;
		type = mType;
		seqNo = no;
	}
	
//aborting and ack msgs
	public Message(int senderID, int receiverID, MsgType mType, int name, MsgType iType) {
		this.sender = senderID;
		this.receiver = receiverID;
		fName = name;
		type = mType;
		initialRequestType = iType;
	}
	
//release write lock
	public Message(int sender, int receiver, MsgType mType, DataFile f, String data, MsgType iType)
	{
		this.sender = sender;
		this.receiver = receiver;
		type = mType;
		file = f;
		content = data;
		initialRequestType = iType;
	}
	
	//release read lock
	public Message(int sender, int receiver, MsgType mType, MsgType iType, int name)
	{
		this.sender = sender;
		this.receiver = receiver;
		type = mType;
		initialRequestType = iType;
		fName = name;
	}
	
	//request content
	public Message(int sender, int receiver, MsgType mType,int name)
	{
		this.sender = sender;
		this.receiver = receiver;
		type = mType;
		fName = name;
	}
	
	//reply content
	public Message(int sender, int receiver, MsgType mType,String data,int name)
	{
		this.sender = sender;
		this.receiver = receiver;
		type = mType;
		content = data;
		fName = name;
	}
	
	@Override
	public boolean equals(Object obj) 
	{
		if(obj!=null && obj instanceof Message)
		{
			Message mm = (Message)obj;
			if((mm.type == this.type) && (mm.sender == this.sender) && (mm.receiver == this.receiver) && (mm.fName == this.fName))
			{
				return true;
			}
		}
		return false;
	}
}


enum MsgType {
	
	WRITE_REQ, READ_REQ, REPLY, ACK, RELEASE_WRITE_LOCK, RELEASE_READ_LOCK, REPLY_CONTENT, REQUEST_CONTENT
}
