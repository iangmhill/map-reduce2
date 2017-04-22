package hadoop;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;
import java.util.ArrayList;

public class Manager extends UnicastRemoteObject implements ManagerInterface {
  private ArrayList<Reducer> reducers;
  private ArrayList<Mapper> mappers;
  private int pingsInProgress = 0;
  public Manager() throws RemoteException {
    mappers = new ArrayList<Mapper>();
    reducers = new ArrayList<Reducer>();
  }

  public MapperInterface createMapTask() {
    Mapper mapper = null;
    try {
      mapper = new Mapper();
      mappers.add(mapper);
      System.out.println("Map task created");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    return mapper;
  }

  public ReducerInterface createReduceTask(String key, MasterInterface master) {
    Reducer reducer = null;
    try {
      reducer = new Reducer(key, master);
      reducers.add(reducer);
      System.out.println("Reduce task created");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    return reducer;
  }

  public static void main(String args[]) {
    try {
      // Initialize account information
      String selfPort = args[0];
      String selfIp = InetAddress.getLocalHost().getHostAddress();

      // Get the local registry
      Registry registry = LocateRegistry.getRegistry(selfIp, Integer.parseInt(selfPort));

      // Set up account
      Manager manager = new Manager();

      // Bind the remote object's stub in the registry
      registry.bind("Manager", manager);
      System.out.println("Manager ready");

    } catch (Exception e) {
      System.err.println("Connection exception: " + e.toString());
      e.printStackTrace();
    }
  }
}