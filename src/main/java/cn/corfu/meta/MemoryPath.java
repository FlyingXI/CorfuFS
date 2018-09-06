package cn.corfu.meta;

import java.util.Date;

import ru.serce.jnrfuse.struct.FileStat;

public abstract class MemoryPath {

	protected String name;
	protected Date createDate;
	protected Date modifiedDate;
	protected String permission;
	protected String local;
	protected MemoryDirectory parent;

	protected MemoryPath(String name) {
		this(name, null);
	}

	protected MemoryPath(String name, MemoryDirectory parent) {
		this.name = name;
		this.parent = parent;
	}

	public synchronized void delete() {
		if (parent != null) {
			parent.deleteChild(this);
			parent = null;
		}
	}

	protected MemoryPath find(String path) {
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		if (path.equals(name) || path.isEmpty()) {
			return this;
		}
		return null;
	}

	public abstract void getattr(FileStat stat);

	public void rename(String newName) {
		while (newName.startsWith("/")) {
			newName = newName.substring(1);
		}
		name = newName;
	}
}