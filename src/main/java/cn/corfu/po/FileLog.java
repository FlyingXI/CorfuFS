package cn.corfu.po;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

public class FileLog implements Serializable {

	private String name;
	private String path;
	private String operation;
	private Date modifieldTime;
	private Date createTime;
	private byte[] value;
	private long offset = 0;

	private String serialVersionUID;

	public FileLog(String name, String path, String operation, byte[] value) {
		super();
		this.name = name;
		this.path = path;
		this.operation = operation;
		this.value = value;
	}

	public FileLog() {
		super();
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public Date getModifieldTime() {
		return modifieldTime;
	}

	public void setModifieldTime(Date modifieldTime) {
		this.modifieldTime = modifieldTime;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	@Override
	public String toString() {
		return "FileLog [name=" + name + ", path=" + path + ", operation=" + operation + ", value=" + value + "]";
	}

	public void serialize(ByteBuf b) {
		// TODO Auto-generated method stub
		try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
			try (ObjectOutputStream oos = new ObjectOutputStream(bbos)) {
				oos.writeObject(this);
			}
		} catch (IOException ie) {
			// log.error("Exception during serialization!", ie);
		}

	}

}
