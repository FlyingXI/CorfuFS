package cn.corfu.service;

import cn.corfu.meta.MemoryDirectory;
import cn.corfu.meta.MemoryFile;
import cn.corfu.meta.MemoryPath;
import cn.corfu.po.FileLog;
import ru.serce.jnrfuse.ErrorCodes;

public class OSManager {
	private static OSManager osmanager = new OSManager();
	private MemoryDirectory rootDirectory;

	private OSManager() {
		rootDirectory = new MemoryDirectory("");
	}

	public static OSManager getOSManager() {
		return osmanager;
	}

	public String getLastComponent(String path) {
		while (path.substring(path.length() - 1).equals("/")) {
			path = path.substring(0, path.length() - 1);
		}
		if (path.isEmpty()) {
			return "";
		}
		return path.substring(path.lastIndexOf("/") + 1);
	}

	public MemoryPath getParentPath(String path) {
		return rootDirectory.find(path.substring(0, path.lastIndexOf("/")));
	}

	public MemoryPath getPath(String path) {
		return rootDirectory.find(path);
	}

	public int execute(FileLog fl) {
		switch (fl.getOperation()) {
		case "mkdir":
			return mkdir(fl);
		case "create":
			return mkfile(fl);
		case "mv":
			return rnname(fl);
		case "write":
			return write(fl);
		case "rmdir":
			return rmdir(fl);
		case "rm":
			return unlink(fl);
		}

		return -1;
	}

	public int mkdir(FileLog fl) {
		String path = fl.getName();
		if (getPath(path) != null) {
			return -ErrorCodes.EEXIST();
		}
		MemoryPath parent = getParentPath(path);
		if (parent instanceof MemoryDirectory) {
			((MemoryDirectory) parent).mkdir(getLastComponent(path));
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	public int mkfile(FileLog fl) {
		String path = fl.getName();
		if (getPath(path) != null) {
			return -ErrorCodes.EEXIST();
		}
		MemoryPath parent = getParentPath(path);
		if (parent instanceof MemoryDirectory) {
			((MemoryDirectory) parent).mkfile(getLastComponent(path));
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	public int rnname(FileLog fl) {
		String path = fl.getName();
		String newName = new String(fl.getValue());

		MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		MemoryPath newParent = getParentPath(newName);
		if (newParent == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(newParent instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		p.delete();
		p.rename(newName.substring(newName.lastIndexOf("/")));
		((MemoryDirectory) newParent).add(p);
		return 0;
	}

	public int write(FileLog fl) {
		String path = fl.getName();
		byte[] buf = fl.getValue();
		long size = fl.getValue().length;
		long offset = fl.getOffset();

		MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}

		return ((MemoryFile) p).write(buf, size, offset);
	}

	public int rmdir(FileLog fl) {
		String path = fl.getName();
		MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		p.delete();
		return 0;
	}

	public int unlink(FileLog fl) {
		String path = fl.getName();
		MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		p.delete();
		return 0;
	}
}
