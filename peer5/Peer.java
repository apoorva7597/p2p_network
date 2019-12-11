import java.io.*;
import java.net.*;
import java.util.*;

public class Peer extends Thread {
	// public String filename = "test.pdf";
	public int fileOwnerPortNum;
	public int downloadNeighborPortNum;
	public int uploadNeighborPortNum;
	public String option;
	public static int chunkNum;
	public static ArrayList<byte[]> arraylist = new ArrayList<byte[]>();;
	public static ArrayList<Integer> presentChunkList = new ArrayList<Integer>();

	// constants
	public String HOST = "localhost";
	public String FILE_NAME = "test.pdf";

	public static final int GET_LIST_CODE = -101;
	public static final int DOWNLOAD_COMPLETE_CODE = -102;
	public static final int DISCARD_CODE = -103;
	public static final int LOOP_AGAIN = -104;
	public static final int READ_NEXT_CODE = -105;
	public static final int SERVER_DOWNLOAD_COMPLETE_CODE = -106;

	public Peer(int PortNum, String option) throws IOException {
		if (option.equals("DNS")) {
			this.fileOwnerPortNum = PortNum;
			this.option = option;
		} else if (option.equals("DNB")) {
			this.downloadNeighborPortNum = PortNum;
			this.option = option;
		} else if (option.equals("UPLOAD")) {
			this.uploadNeighborPortNum = PortNum;
			this.option = option;
		}
	}

	public void recieveServerChunk() {
		String savePathsmall = null;
		Socket socket = null;
		DataInputStream in = null;
		DataOutputStream out = null;
		try {
			socket = new Socket(HOST, this.fileOwnerPortNum);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
			out = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		long len = 0;
		try {
			out.writeUTF(this.FILE_NAME);
			out.flush();
			len = in.readLong();
			int chunknum = in.readInt();
			this.chunkNum = chunknum;
			System.out.println("Started receiving chunk: " + this.FILE_NAME);
			System.out.println("Size of the file " + len + "B");
			System.out.println("The total chunknum is " + chunknum);
			for (int i = 0; i < chunknum; i++) {
				presentChunkList.add(0);
			}
			int c = 0;
			while (c < presentChunkList.size()) {
				out.writeInt(this.GET_LIST_CODE);
				out.flush();

				ArrayList<Integer> serverChunkList = new ArrayList<Integer>(chunknum);
				for (int i = 0; i < chunknum; i++) {
					serverChunkList.add(in.readInt());
				}

				ArrayList<Integer> printMyList = new ArrayList<Integer>(chunknum);
				for(int i=0;i < chunknum; i++) {
					if(presentChunkList.get(i) == 1) {
						printMyList.add(i);
					}
				}
				ArrayList<Integer> printServerList = new ArrayList<Integer>(chunknum);
				for(int i=0;i < chunknum; i++) {
					if(serverChunkList.get(i) == 1) {
						printServerList.add(i);
					}
				}

				System.out.println("Already has:" + printMyList);
				System.out.println("Fileowner has chunks:" + printServerList);

				ArrayList<Integer> clientLackArray = new ArrayList<Integer>();
				for (int i = 0; i < chunknum; i++) {
					if ((serverChunkList.get(i) - presentChunkList.get(i)) == 1) {
						clientLackArray.add(i);
					}
				}

				if (clientLackArray.size() == 0) {
					out.writeInt(SERVER_DOWNLOAD_COMPLETE_CODE);
					break;
				}
				int random = genNextChunkNumber(clientLackArray.size());
				int requestnum = clientLackArray.get(random);
				out.writeInt(requestnum);
				out.flush();
				System.out.println("sending chunknum " + requestnum + " to server");
				int readChunkSize = in.readInt();
				byte[] buf = new byte[readChunkSize];
				in.read(buf);
				System.out.println("Recieveing chunk" + requestnum + " from server!");
				arraylist.add(buf);
				savePathsmall = "./temp/" + requestnum;
				DataOutputStream fileOut = new DataOutputStream(
						new BufferedOutputStream(new BufferedOutputStream(new FileOutputStream(savePathsmall))));
				fileOut.write(arraylist.get(arraylist.size() - 1));

				fileOut.close();
				presentChunkList.set(requestnum, 1);
				int counter = 0;
				for (int i = 0; i < presentChunkList.size(); i++) {
					if (presentChunkList.get(i) == 1) {
						counter++;
					}
				}
				c = counter;
				if (c == presentChunkList.size()) {
					out.writeInt(DOWNLOAD_COMPLETE_CODE);
					out.flush();
				} else {
					out.writeInt(DISCARD_CODE);
					out.flush();
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
			}
		} catch (Exception e) {
			return;
		} finally {
			try {
				closeConnection(in, out, socket, null);
			} catch (IOException ioException) {
				System.out.println("Issue while closing connection");
			}
		}
	}

	public void recievePeerChunk() {
		String savePathsmall = null;
		Socket socket = null;
		DataInputStream in = null;
		DataOutputStream out = null;
		try {
			sleep(1500);
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
		try {
			socket = new Socket(HOST, this.downloadNeighborPortNum);
			System.out.println("Connected to downloadNeighbor is established.");
		} catch (UnknownHostException e1) {
			System.out.println(
					"Connection to download neighbor couldn't be established, will try to reconnect in 2 seconds.");
			try {
				sleep(2000);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
			this.recievePeerChunk();
		} catch (IOException e1) {
			System.out.println(
					"Connection to download neighbor couldn't be established, will try to reconnect in 2 seconds.");
			try {
				sleep(2000);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
			this.recievePeerChunk();
			return;
		}
		try {
			in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
			out = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			int c = 0;
			while (c < presentChunkList.size()) {

				// ArrayList<Integer> downloadNeighbormarkarray = getChunkList(in,
				// presentChunkList.size());
				out.writeInt(this.GET_LIST_CODE);
				ArrayList<Integer> downloadNeighbormarkarray = new ArrayList<Integer>(presentChunkList.size());
				for (int i = 0; i < presentChunkList.size(); i++) {
					downloadNeighbormarkarray.add(in.readInt());
				}

				ArrayList<Integer> printMyList = new ArrayList<Integer>(presentChunkList.size());
				for(int i=0;i < presentChunkList.size(); i++) {
					if(presentChunkList.get(i) == 1) {
						printMyList.add(i);
					}
				}
				ArrayList<Integer> printPeerList = new ArrayList<Integer>(presentChunkList.size());
				for(int i=0;i < presentChunkList.size(); i++) {
					if(downloadNeighbormarkarray.get(i) == 1) {
						printPeerList.add(i);
					}
				}

				System.out.println("Already has:" + printMyList);
				System.out.println("Fileowner has chunks:" + printPeerList);

				// System.out.println("Already has:" + presentChunkList);
				// System.out.println("DownloadNeighbor has chunks : " + downloadNeighbormarkarray);
				ArrayList<Integer> clientLackArray = new ArrayList<Integer>();
				for (int i = 0; i < presentChunkList.size(); i++) {
					if ((downloadNeighbormarkarray.get(i) - presentChunkList.get(i)) == 1) {
						clientLackArray.add(i);
					}
				}
				if (clientLackArray.isEmpty()) {
					int counter = 0;
					for (int i = 0; i < presentChunkList.size(); i++) {
						if (presentChunkList.get(i) == 1) {
							counter++;
						}
					}
					c = counter;
					if (c == presentChunkList.size()) {
						out.writeInt(READ_NEXT_CODE);
						out.writeInt(DOWNLOAD_COMPLETE_CODE);
						out.flush();

						System.out.println(
								"Download completed successfully. Will disconnect from downloadneighbor soon.");
					} else {
						out.writeInt(LOOP_AGAIN);
						System.out.println("Downloadneighbor has no chunk i require.");
						sleep(1000);
					}
					continue;
				} else {
					out.writeInt(READ_NEXT_CODE);
					int requestnum;
					int rnd = new Random().nextInt(clientLackArray.size());
					requestnum = clientLackArray.get(rnd);
					out.writeInt(requestnum);
					out.flush();
					System.out.println("Sending chunknum " + requestnum + " to downloadNeighbor");
					int readChunkSize = in.readInt();
					byte[] buf = new byte[readChunkSize];
					in.read(buf);
					System.out.println("Recieveing chunk " + requestnum + " from downloadNeighbor.");
					arraylist.add(buf);
					savePathsmall = "./temp/" + requestnum;
					DataOutputStream fileOut = new DataOutputStream(
							new BufferedOutputStream(new BufferedOutputStream(new FileOutputStream(savePathsmall))));
					fileOut.write(arraylist.get(arraylist.size() - 1));
					fileOut.close();
					try {
						sleep(1000);
					} catch (Exception ee) {
						;
					}
					presentChunkList.set(requestnum, 1);
					int counter = 0;
					for (int i = 0; i < presentChunkList.size(); i++) {
						if (presentChunkList.get(i) == 1) {
							counter++;
						}
					}
					c = counter;
					if (c == presentChunkList.size()) {
						out.writeInt(DOWNLOAD_COMPLETE_CODE);
						out.flush();
						System.out.println(
								"Download completed successfully. Will disconnect from downloadneighbor soon.");
					} else {
						out.writeInt(DISCARD_CODE);
						out.flush();
					}
				}
			}
		} catch (Exception e) {

			return;
		} finally {
			// Close connections
			try {
				closeConnection(in, out, socket, null);
			} catch (IOException ioException) {
				System.out.println("Issue while closing connection");
			}
		}
	}

	public void sendPeerChunk() {
		try {
			sleep(1500);
		} catch (InterruptedException e) {
		}
		ServerSocket ss = null;
		DataOutputStream out = null;
		DataInputStream in = null;
		DataInputStream inSocket = null;
		Socket socket = null;
		try {
			ss = new ServerSocket(this.uploadNeighborPortNum);
			System.out.println("Peer is waiting to connect with uploadNeighbor:");
			socket = ss.accept();
			inSocket = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
			out = new DataOutputStream(socket.getOutputStream());
			while (true) {
				if (inSocket.readInt() == this.GET_LIST_CODE) {
					for (int j = 0; j < presentChunkList.size(); j++) {
						out.writeInt(presentChunkList.get(j));
						out.flush();
					}
				}
				int s = inSocket.readInt();
				if (s == READ_NEXT_CODE) {
					int requestnumber = inSocket.readInt();
					if (requestnumber == DOWNLOAD_COMPLETE_CODE) {
						break;
					}
					System.out.println("File request number from uploadNeighbor:" + requestnumber);
					String filename = "./temp/" + requestnumber;
					File file = new File(filename);
					in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
					byte[] bufArray = new byte[(int) file.length()];
					out.writeInt((int) file.length());
					out.flush();
					in.read(bufArray);
					out.write(bufArray, 0, (int) file.length());
					System.out.println("Sending file chunk" + requestnumber + " to uploadNeighbor.");
					out.flush();
					if (inSocket.readInt() == DOWNLOAD_COMPLETE_CODE) {
						break;
					}
				}
			}
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		} finally {
			try {
				closeConnection(in, out, socket, ss);
				inSocket.close();
			} catch (Exception e) {
			}
		}
	}

	public ArrayList<Integer> getChunkList(DataInputStream in, int chunknum) {
		ArrayList<Integer> chunkList = new ArrayList<Integer>(chunknum);
		try {
			for (int i = 0; i < chunknum; i++) {
				chunkList.add(in.readInt());
			}
		} catch (Exception e) {
			System.out.println("Trying to read chunk list");
		}
		return chunkList;
	}

	// public ArrayList<Integer> getMissingChunkList(DataInputStream in, int
	// chunknum) {
	// ArrayList<Integer> chunkList =new ArrayList<Integer>(chunknum);
	// for(int i=0;i<chunknum;i++){
	// if((serverChunkList.get(i)-clientmarkarray.get(i))==1){
	// clientLackArray.add(i);
	// }
	// }
	// return chunkList;
	// }

	public int genNextChunkNumber(int listSize) {
		Random random = new Random();
		int randomInt = random.nextInt(listSize);
		return randomInt;
	}

	public void closeConnection(DataInputStream in, DataOutputStream out, Socket socket, ServerSocket ss)
			throws IOException {

		try {
			if (out != null)
				out.close();
		} catch (IOException e) {
			System.out.println("Connection already closed");
		}
		try {
			if (in != null)
				in.close();
		} catch (IOException e) {
			System.out.println("Connection already closed");
		}
		try {
			if (socket != null)
				socket.close();
		} catch (IOException e) {
			System.out.println("Connection already closed");
		}
		try {
			if (ss != null)
				ss.close();
			System.out.println("Connection between peer and uploadNeighbor is closed now.");
		} catch (IOException e) {
			System.out.println("Connection already closed");
		}
		System.out.println("Connection between peer and downloadNeighbor is closed now.");
		Merger.mergeFileHelper(chunkNum, FILE_NAME);
	}

 @Override
	public void run() {
		try {
			if (option.equals("DNS")) {
				this.recieveServerChunk();
			} else if (option.equals("DNB") || option.equals("UPLOAD")) {
				this.startPeer();
			}
		} catch (Exception e) {
			System.out.println("Exiting...");
		}
	}

	public void startPeer() {
		if (option.equals("DNB")) {
			this.recievePeerChunk();
		} else if (option.equals("UPLOAD")) {
			this.sendPeerChunk();
		}
	}

	public static void main(String[] args) throws IOException {

		int port1 = Integer.parseInt(args[0]);
		int port2 = Integer.parseInt(args[1]);
		int port3 = Integer.parseInt(args[2]);

		Peer peer1 = new Peer(port1, "DNS");
		Peer peer2 = new Peer(port2, "DNB");
		Peer peer3 = new Peer(port3, "UPLOAD");
		peer1.start();
		peer2.start();
		peer3.start();
	}
}
