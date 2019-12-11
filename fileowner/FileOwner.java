
import java.io.*;
import java.net.*;
import java.util.*;

public class FileOwner extends Thread {
  public ServerSocket ss = null;
  public Socket connection = null;
  DataOutputStream out = null; 
  DataInputStream in = null; 
  public int port;
  public String filename;
  public List<byte[]> arraylist;
  public List<Integer> chunkList;

  // constants
  public static final int GET_LIST_CODE = -101;
  public static final int DOWNLOAD_COMPLETE_CODE = -102;
  public static final int SERVER_DOWNLOAD_COMPLETE_CODE = -106;
  private final int chunkSize = 102400;

// public FileOwner(ServerSocket sSocket, Socket s, DataInputStream dis, DataOutputStream dos) {
//     this.ss = sSocket;
//     this.connection = s;
//     this.in = dis;
//     this.out = dos;  
// }

public FileOwner(int port) {
  this.port = port; 
}

  @Override
  public void run() {
    DataOutputStream out = null;
    DataInputStream in = null;
    DataInputStream inSocket = null;
    Socket socket = null;
    try {

      ss = new ServerSocket(this.port);
      System.out.println("Server is requesting connection establishment");
      socket = ss.accept();
      inSocket = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      out = new DataOutputStream(socket.getOutputStream());
      String filename = inSocket.readUTF();
      System.out.println("filename:" + filename);
      this.filename = filename;
      File file = new File(filename);
      out.writeLong((long) file.length());
      long filelength = (long) file.length();
      System.out.println("file length:" + filelength);
      out.flush();
      createChunks(filelength);
      // in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
      // ArrayList<byte[]> arraylist = new ArrayList<byte[]>();
      // ArrayList<Integer> chunkList = new ArrayList<Integer>();
      // int remainlength = (int) filelength;
      // while (remainlength > 0) {
      //   if (remainlength <= this.chunkSize) {
      //     byte[] Chunk = new byte[remainlength];
      //     in.read(Chunk);
      //     remainlength = 0;
      //     arraylist.add(Chunk);
      //     chunkList.add(1);
      //   } else {
      //     byte[] Chunk = new byte[this.chunkSize];
      //     in.read(Chunk);
      //     remainlength = remainlength - this.chunkSize;
      //     arraylist.add(Chunk);
      //     chunkList.add(1);
      //   }
      // }
      System.out.println("chunksize:" + chunkSize);
      System.out.println("Total chunk number: " + arraylist.size());
      System.out.println("FileOwner has " + chunkList);
      out.writeInt(chunkList.size());// send chunknum to receiver
      out.flush();
      while (true) {
        if (inSocket.readInt() == GET_LIST_CODE) {
          for (int j = 0; j < chunkList.size(); j++) {
            // System.out.println(chunkList.get(j));
            out.writeInt(chunkList.get(j));
            out.flush();
          }
        }
        int a = inSocket.readInt();
        if (a == SERVER_DOWNLOAD_COMPLETE_CODE) {
          break;
        }
        int requestnumber = a;
        System.out.println("requesting chunk number from peer:" + requestnumber);
        out.writeInt(arraylist.get(requestnumber).length);
        // System.out.println("arraylist.get(requestnumber).length"+arraylist.get(requestnumber).length);
        out.write(arraylist.get(requestnumber), 0, arraylist.get(requestnumber).length);
        out.flush();
        System.out.println("Send file chunk" + requestnumber + " to peer.");

        if (inSocket.readInt() == DOWNLOAD_COMPLETE_CODE) {
          break;
        }
      }
    } catch (FileNotFoundException e) {
    } catch (IOException e) {
    } finally {
      try {
        closeConnection(in, out, socket, ss);
        inSocket.close();
      } catch (Exception e) {}
    }
  }

  public void createChunks(long flength) {
    
    try{
      FileInputStream fstream = new FileInputStream(this.filename);
      DataInputStream dstream = new DataInputStream(fstream);

      arraylist = new ArrayList<byte[]>();
      chunkList = new ArrayList<Integer>();
      int flen = (int) flength;
      while(flen > chunkSize) {
        byte[] chunk = new byte[chunkSize];
        dstream.read(chunk);
        flen -= chunkSize;
        // System.out.println(chunkSize);
        arraylist.add(chunk);
        chunkList.add(1);
      }
      if(flen > 0){
          byte[] chunk = new byte[flen];
          dstream.read(chunk);
          flen = 0;
          arraylist.add(chunk);
          chunkList.add(1);
      }
    }catch(FileNotFoundException fnf) {
      System.out.println("File does not exist on the server");
    }catch(Exception e) {}

  // for(int i=1;i<=arraylist.size();i++){
  //     presentChunksList.add(i);
  // }
  }

  public void closeConnection(DataInputStream in, DataOutputStream out, Socket socket, ServerSocket ss) throws IOException {
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
			System.out.println("Server closed");
		} catch (IOException e) {
			System.out.println("Connection already closed");
		}   
		System.out.println("Connection is closed now.");
	
	}

  public static void main(String[] args) {
    String[] temp = null;
    try {
      FileInputStream fis = new FileInputStream("config");
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String line = null;
      line = br.readLine();
      temp = line.split(" ");
      br.close();
    } catch (Exception e) {}
    
    int[] port = new int[5];
    for(int i=0; i<5; i++) {
      port[i] = Integer.parseInt(temp[i+1]);
    }

    FileOwner[] fos = new FileOwner[5];
    for(int i=0; i<5; i++) {
      fos[i] = new FileOwner(port[i]);
      fos[i].start();
    }
  }
}