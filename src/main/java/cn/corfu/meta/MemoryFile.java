package cn.corfu.meta;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.Instant;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.Timespec;

public class MemoryFile extends MemoryPath {
	private ByteBuffer contents = ByteBuffer.allocate(0);

	private MemoryFile(String name) {
		super(name);
	}

	MemoryFile(String name, MemoryDirectory parent) {
		super(name, parent);
	}

	public MemoryFile(String name, String text) {
		super(name);
		try {
			byte[] contentBytes = text.getBytes("UTF-8");
			contents = ByteBuffer.wrap(contentBytes);
		} catch (UnsupportedEncodingException e) {
			// Not going to happen
		}
	}

	private void fillTime(Instant instant, Timespec timespec) {
		timespec.tv_sec.set(instant.getEpochSecond());
		timespec.tv_nsec.set(instant.getNano());
	}

	@Override
	public void getattr(FileStat stat) {
		stat.st_mode.set(FileStat.S_IFREG | 0775);
		stat.st_size.set(contents.capacity());
		stat.st_nlink.set(1);
		stat.st_gid.set(0);
		stat.st_uid.set(0);
		stat.st_blocks.set((contents.capacity() + 511L) / 512L);
		fillTime(Instant.now(), stat.st_atim);
		fillTime(new Time(this.modifiedDate.getTime()).toInstant(), stat.st_mtim);
		fillTime(new Time(this.createDate.getTime()).toInstant(), stat.st_ctim);

	}

	public int read(Pointer buffer, long size, long offset) {
		int bytesToRead = (int) Math.min(contents.capacity() - offset, size);
		byte[] bytesRead = new byte[bytesToRead];
		synchronized (this) {
			contents.position((int) offset);
			contents.get(bytesRead, 0, bytesToRead);
			buffer.put(0, bytesRead, 0, bytesToRead);
			contents.position(0); // Rewind
		}
		return bytesToRead;
	}

	public synchronized void truncate(long size) {
		if (size < contents.capacity()) {
			// Need to create a new, smaller buffer
			ByteBuffer newContents = ByteBuffer.allocate((int) size);
			byte[] bytesRead = new byte[(int) size];
			contents.get(bytesRead);
			newContents.put(bytesRead);
			contents = newContents;
		}
	}

	public int write(byte[] buffer, long bufSize, long writeOffset) {
		int maxWriteIndex = (int) (writeOffset + bufSize);
		// byte[] bytesToWrite = new byte[(int) bufSize];
		synchronized (this) {
			if (maxWriteIndex > contents.capacity()) {
				// Need to create a new, larger buffer
				ByteBuffer newContents = ByteBuffer.allocate(maxWriteIndex);
				newContents.put(contents);
				contents = newContents;
			}
			// buffer.get(0, bytesToWrite, 0, (int) bufSize);
			contents.position((int) writeOffset);
			contents.put(buffer);
			contents.position(0); // Rewind
		}
		return (int) bufSize;
	}
}
