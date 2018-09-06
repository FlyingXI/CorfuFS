package cn.corfu.meta;

import java.util.ArrayList;
import java.util.List;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

public class MemoryDirectory extends MemoryPath {

	private List<MemoryPath> contents = new ArrayList<>();

	public MemoryDirectory(String name) {
		super(name);
	}

	public MemoryDirectory(String name, MemoryDirectory parent) {
		super(name, parent);
	}

	public synchronized void add(MemoryPath p) {
		contents.add(p);
		p.parent = this;
	}

	public synchronized void deleteChild(MemoryPath child) {
		contents.remove(child);
	}

	@Override
	public MemoryPath find(String path) {
		if (super.find(path) != null) {
			return super.find(path);
		}
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		synchronized (this) {
			if (!path.contains("/")) {
				for (MemoryPath p : contents) {
					if (p.name.equals(path)) {
						return p;
					}
				}
				return null;
			}
			String nextName = path.substring(0, path.indexOf("/"));
			String rest = path.substring(path.indexOf("/"));
			for (MemoryPath p : contents) {
				if (p.name.equals(nextName)) {
					return p.find(rest);
				}
			}
		}
		return null;
	}

	@Override
	public void getattr(FileStat stat) {
		stat.st_mode.set(FileStat.S_IFDIR | 0775);
		stat.st_nlink.set(1);
	}

	public synchronized void mkdir(String lastComponent) {
		contents.add(new MemoryDirectory(lastComponent, this));
	}

	public synchronized void mkfile(String lastComponent) {
		contents.add(new MemoryFile(lastComponent, this));
	}

	public synchronized void read(Pointer buf, FuseFillDir filler) {
		for (MemoryPath p : contents) {
			filler.apply(buf, p.name, null, 0);
		}
	}
}
