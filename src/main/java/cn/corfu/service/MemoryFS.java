package cn.corfu.service;

import static jnr.ffi.Platform.OS.WINDOWS;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;

import cn.corfu.meta.MemoryDirectory;
import cn.corfu.meta.MemoryFile;
import cn.corfu.meta.MemoryPath;
import cn.corfu.po.CorfuClient;
import cn.corfu.po.FileLog;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;

public class MemoryFS extends FuseStubFS {

	public static void main(String[] args) {
		MemoryFS memfs = new MemoryFS();

		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(6000);
						memfs.init();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});

		t1.start();
		try {
			String path;
			switch (Platform.getNativePlatform().getOS()) {
			case WINDOWS:
				path = "J:\\";
				break;
			default:
				path = "/home/fanjin/corfu/test1";
			}
			memfs.mount(Paths.get(path), true, true);
		} finally {
			memfs.umount();
		}
	}

	private CorfuClient client = new CorfuClient("192.168.123.117:9001");
	private OSManager osmanager = OSManager.getOSManager();

	public synchronized int loopExeCorfu(long seq) {
		long currentSeq;
		int result = 0;
		while ((currentSeq = client.getLocalSeq()) <= seq) {
			try {
				FileLog fl = client.read(currentSeq);
				result = osmanager.execute(fl);
				client.setLocalSeq(++currentSeq);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return result;
	}

	public MemoryFS() {
		// Sprinkle some files around
		init();

		System.out.println("init finished...........");
		System.out.println(client.getServiceSeq() + ":::" + client.getLocalSeq());

	}

	@Override
	public int create(String path, @mode_t long mode, FuseFileInfo fi) {
		FileLog fl = new FileLog();
		fl.setName(path);
		fl.setModifieldTime(new Date());
		fl.setOperation("create");
		fl.setCreateTime(new Date());

		try {
			long seq = client.write(fl);
			return loopExeCorfu(seq);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int getattr(String path, FileStat stat) {
		MemoryPath p = osmanager.getPath(path);
		System.out.println("======" + path);
		if (p != null) {
			p.getattr(stat);
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int mkdir(String path, @mode_t long mode) {
		FileLog fileLog = new FileLog(path, null, "mkdir", null);

		fileLog.setModifieldTime(new Date());
		fileLog.setCreateTime(new Date());

		try {
			long seq = client.write(fileLog);
			return loopExeCorfu(seq);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;

	}

	@Override
	public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
		MemoryPath p = osmanager.getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		return ((MemoryFile) p).read(buf, size, offset);
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
		MemoryPath p = osmanager.getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		filter.apply(buf, ".", null, 0);
		filter.apply(buf, "..", null, 0);
		((MemoryDirectory) p).read(buf, filter);
		return 0;
	}

	@Override
	public int statfs(String path, Statvfs stbuf) {
		if (Platform.getNativePlatform().getOS() == WINDOWS) {
			// statfs needs to be implemented on Windows in order to allow for
			// copying
			// data from other devices because winfsp calculates the volume size
			// based
			// on the statvfs call.
			// see
			// https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
			if ("/".equals(path)) {
				stbuf.f_blocks.set(1024 * 1024); // total data blocks in file
													// system
				stbuf.f_frsize.set(1024); // fs block size
				stbuf.f_bfree.set(1024 * 1024); // free blocks in fs
			}
		}
		return super.statfs(path, stbuf);
	}

	@Override
	public int rename(String path, String newName) {
		FileLog fileLog = new FileLog(path, "", "mv", newName.getBytes());

		fileLog.setModifieldTime(new Date());
		try {
			long seq = client.write(fileLog);
			return loopExeCorfu(seq);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int rmdir(String path) {
		FileLog fileLog = new FileLog(path, "", "rmdir", null);

		try {
			long seq = client.write(fileLog);
			return loopExeCorfu(seq);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int truncate(String path, long offset) {
		MemoryPath p = osmanager.getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		((MemoryFile) p).truncate(offset);
		return 0;
	}

	@Override
	public int unlink(String path) {
		FileLog fileLog = new FileLog(path, "", "rm", null);

		try {
			long seq = client.write(fileLog);
			return loopExeCorfu(seq);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int open(String path, FuseFileInfo fi) {
		return 0;
	}

	@Override
	public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {

		byte[] bytesToWrite = new byte[(int) size];
		buf.get(0, bytesToWrite, 0, (int) size);

		FileLog fileLog = new FileLog(path, "", "write", bytesToWrite);

		fileLog.setModifieldTime(new Date());
		try {
			long seq = client.write(fileLog);
			return loopExeCorfu(seq);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;

	}

	public synchronized void init() {
		long seq = 0;
		while (client.getServiceSeq() >= (seq = client.getLocalSeq())) {
			try {
				FileLog fl = client.read(seq);
				osmanager.execute(fl);
				client.setLocalSeq(++seq);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
