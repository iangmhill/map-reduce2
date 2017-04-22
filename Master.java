package hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class Master extends UnicastRemoteObject implements MasterInterface {
  private static Hashtable<String, ManagerInterface> nodeDirectory;
  // private static Hashtable<String, iMapper> mapperDirectory;
  // private static Hashtable<String, iReducer> reducerDirectory;
  private static String[] nodeIds;
  private static Master master;
  private Hashtable<String, ReducerInterface> reducerMapping;
  private int nextMapperIndex = 0;
  private int nextReducerIndex = 0;
  private int mapperTasksRunning = 0;
  private int reducerTasksRunning = 0;
  private boolean fileRead = false;
  private BufferedWriter bw;
  private ArrayList<MapperInterface> completedMappers;

  public Master() throws RemoteException {
    reducerMapping = new Hashtable<String, ReducerInterface>();
    completedMappers = new ArrayList<MapperInterface>();
  }

  public MapperInterface[] getMappers(ReducerInterface reducer, int index) {
    if (mapperTasksRunning == 0 && fileRead && index == completedMappers.size()) {
      try {
        reducer.terminate();
      } catch (Exception e) {
        System.err.println("Reducer could not terminate: " + e.toString());
      }
    }
    List<MapperInterface> mappers = completedMappers.subList(index, completedMappers.size());
    return mappers.toArray(new MapperInterface[mappers.size()]);
  }

  public void markMapperDone(MapperInterface mapper, String[] keys) {
    mapperTasksRunning -= 1;
    completedMappers.add(mapper);
    for (int i = 0; i < keys.length; i++) {
      if (!reducerMapping.containsKey(keys[i])) {
        try {
          ReducerInterface reducer = nodeDirectory
              .get(nodeIds[nextReducerIndex])
              .createReduceTask(keys[i], master);
          reducer.start();
          reducerMapping.put(keys[i], reducer);
          reducerTasksRunning++;
          System.out.println("Reducer started: " + Integer.toString(reducerTasksRunning) + " running");
          nextReducerIndex++;
          if (nextReducerIndex >= nodeIds.length) {
            nextReducerIndex = 0;
          }
        } catch (Exception e) {
          System.err.println("Connection exception: " + e.toString());
        }
      }
    }
    System.out.println("Mapper completed: " + Integer.toString(this.mapperTasksRunning) + " running");
  }

  public void receiveOutput(String key, int value) {
    reducerTasksRunning--;
    System.out.println("Reducer completed: " + Integer.toString(reducerTasksRunning) + " running");
    try {
      if (bw == null) {
        bw = new BufferedWriter(new FileWriter("output.txt"));
        System.out.println("Opened file");
      }
      bw.write(key + ": " + value + "\n");
      if (reducerTasksRunning == 0 && fileRead) {
        bw.close();
        System.out.println("Closed file");
      }
    } catch (IOException x) {
      System.err.format("IOException: %s%n", x);
    }
  }

  public void mapLine(String line) {
    try {
      mapperTasksRunning += 1;
      System.out.println("Mapper started: " + Integer.toString(this.mapperTasksRunning) + " running");

      MapperInterface mapper = nodeDirectory
          .get(nodeIds[nextMapperIndex])
          .createMapTask();
      mapper.processInput(line, master);
      nextMapperIndex++;
      if (nextMapperIndex >= nodeIds.length) {
        nextMapperIndex = 0;
      }
    } catch (Exception e) {
      System.err.println("Connection exception: " + e.toString());
    }
  }

  public void start() {
    try {
      BufferedReader br = new BufferedReader(new FileReader("./text.txt"));
      String line;
      while ((line = br.readLine()) != null) {
        mapLine(line);
      }
      fileRead = true;
    } catch (IOException e) {
      System.out.println("Failed to open file");
    }
  }

  public static void main(String args[]) {
    try {
      // Initialize account information
      String selfPort = args[0];
      String selfIp = InetAddress.getLocalHost().getHostAddress();
      nodeIds = Arrays.copyOfRange(args, 1, args.length);

      // Get the local registry
      Registry registry = LocateRegistry.getRegistry(selfIp, Integer.parseInt(selfPort));

      // Set up account
      master = new Master();

      // Bind the remote object's stub in the registry
      registry.bind("Master", master);
      System.out.println("Master ready");

      // Connect to peers from ips in nodeIds
      nodeDirectory = new Hashtable<String, ManagerInterface>();
      for (int i = 0; i < nodeIds.length; i++) {
        String[] parts = nodeIds[i].split(":");
        String nodeIp = parts[0];
        int nodePort = Integer.parseInt(parts[1]);
        try{
          Registry nodeRegistry = LocateRegistry.getRegistry(nodeIp, nodePort);
          ManagerInterface stub = (ManagerInterface) nodeRegistry.lookup("Manager");
          nodeDirectory.put(nodeIds[i], stub);
          System.out.println("Connection made to: " + nodeIds[i]);
        } catch (RemoteException e) {
          System.err.println("Connection exception: " + e.toString());
          e.printStackTrace();
        }
      }
      master.start();
    } catch (Exception e) {
      System.err.println("Connection exception: " + e.toString());
      e.printStackTrace();
    }
  }
}
