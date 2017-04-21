package hadoop;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;

public class Reducer extends UnicastRemoteObject implements ReducerInterface {
  private String word;
  private int value;
  private MasterInterface masterNode;
  private int mapperIndex;
  private boolean mappersDone;

  public Reducer(String key, MasterInterface master) throws RemoteException {
    word = key;
    value = 0;
    masterNode = master;
    mapperIndex = 0;
    mappersDone = false;
  }

  public void start() {
   while (mappersDone == false){
    pingMaster();
    //pause?
   }
  }

  public void pingMaster(){
    MapperInterface[] mappers = masterNode.getMappers();
    mapperIndex += mappers.length;

    for (int i = 0; i<mappers.length; i++){
      value += mapperIndex[i].getCount(word);
    }

  }

  public int terminate() {
    mappersDone = true;
    try {
      this.masterNode.receiveOutput(word, this.value);
      System.out.println("Reduce task terminated");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    return 0;
  }
}