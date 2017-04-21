package hadoop;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;
import java.util.Set;

public class Mapper extends UnicastRemoteObject implements MapperInterface {
  public Hashtable<String, Integer> freq = null;

  public Mapper() throws RemoteException {
    freq = new Hashtable<String, Integer>();
  }

  public void processInput(String input, MasterInterface master) {
    String[] split = input.toLowerCase().replaceAll("[^a-zA-Z\\s]", "").split("\\s");
    for (int i = 0; i < split.length; i++) {
      if(!freq.containsKey(split[i])){ //if new word
        freq.put(split[i], 1);
      }
      else { // if word already exists in hashmap
        int prevFrequency = freq.get(split[i]);
        freq.put(split[i], ++prevFrequency);
      }
    }
    Set<String> keySet = freq.keySet();
    String[] keys = keySet.toArray(new String[keySet.size()]);
    try {
      master.markMapperDone(this);
      System.out.println("Map task done");
    } catch (Exception e) {
      System.err.println("Failed to get or send to reducers: " + e.toString());
      e.printStackTrace();
    }
  }
}