package cn.corfu.po;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.NodeLocator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

public class CorfuClient {

	private final String endpoint;
	private CorfuRuntime runtime;
	private String state;
	private String streamName = "file";
	private long localSeq = 0;

	public CorfuClient(String endpoint) {
		this.endpoint = endpoint;
		connect(endpoint);
	}

	public CorfuClient(String endpoint, long localSeq) {
		this.endpoint = endpoint;
		connect(endpoint);
		this.localSeq = localSeq;
	}

	public synchronized long getLocalSeq() {
		return localSeq;
	}

	public synchronized void setLocalSeq(long localSeq) {
		this.localSeq = localSeq;
	}

	public CorfuRuntime getRuntime() {
		if (state.equals("CONNECTED")) {
			return runtime;
		}
		connect(endpoint);

		return runtime;
	}

	private void connect(String endpoint) {
		List<NodeLocator> layoutServers = new ArrayList<NodeLocator>();
		layoutServers.add(NodeLocator.parseString(endpoint));
		CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build()
				.setLayoutServers(layoutServers);
		state = "CONNECTED";
		this.runtime = CorfuRuntime.fromParameters(parameters).connect();
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("CorfuClient{");
		sb.append("endpoint='").append(endpoint).append('\'');
		sb.append(", runtime=").append(runtime);
		sb.append(", state='").append(state).append('\'');
		sb.append(", streamName='").append(streamName).append('\'');
		sb.append('}');
		return sb.toString();
	}

	public long write(FileLog fileMetadata) throws IOException {
		if (this.runtime == null) {
			this.connect(endpoint);
		}
		UUID stream = CorfuRuntime.getStreamID(this.streamName);
		StreamsView sv = this.runtime.getStreamsView();
		IStreamView isv = sv.get(stream);

		ByteBuf b = Unpooled.buffer(8);
		ByteBufOutputStream bbos = new ByteBufOutputStream(b);
		ObjectOutputStream oos = new ObjectOutputStream(bbos);
		oos.writeObject(fileMetadata);

		bbos.close();

		return isv.append(b.array());
	}

	public long write(ByteBuffer buf) throws IOException {
		if (this.runtime == null) {
			this.connect(endpoint);
		}
		UUID stream = CorfuRuntime.getStreamID(this.streamName);
		StreamsView sv = this.runtime.getStreamsView();
		IStreamView isv = sv.get(stream);

		return isv.append(buf.array());
	}

	public long getServiceSeq() {
		UUID stream = CorfuRuntime.getStreamID(this.streamName);
		TokenResponse toke = this.runtime.getSequencerView().query(stream);
		return toke.getTokenValue();
	}

	public FileLog read(long seek) throws IOException, ClassNotFoundException {
		if (this.runtime == null) {
			this.connect(endpoint);
		}

		byte[] b = (byte[]) runtime.getAddressSpaceView().read(seek).getPayload(runtime);

		InputStream is = new BufferedInputStream(new ByteArrayInputStream(b));

		ObjectInputStream objInt = new ObjectInputStream(is);
		FileLog fl = (FileLog) objInt.readObject();
		objInt.close();
		return fl;
	}

	public List<FileLog> readAll(long start, long end) throws IOException, ClassNotFoundException {
		start = Math.max(start, 0);
		end = Math.max(end, 1);

		if (start > end) {
			return null;
		}
		List<FileLog> list = new ArrayList<>();

		UUID stream = CorfuRuntime.getStreamID(this.streamName);
		StreamsView sv = this.runtime.getStreamsView();
		IStreamView isv = sv.get(stream);

		// isv.streamUpTo(end);
		isv.remainingUpTo(end);
		isv.seek(start);
		ILogData logData = null;
		while ((logData = isv.current()) != null) {
			isv.seek(++start);

			byte[] b = (byte[]) logData.getPayload(this.runtime);
			System.out.println(b);
			if (b == null)
				break;
			ByteArrayInputStream byteInt = new ByteArrayInputStream(b);
			ObjectInputStream objInt = new ObjectInputStream(byteInt);
			list.add((FileLog) objInt.readObject());
		}

		return list;
	}

}
