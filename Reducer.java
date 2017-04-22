package hadoop;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;

public class Reducer extends UnicastRemoteObject implements ReducerInterface {
  private String word;
  private int value;
  private MasterInterface masterNode;
  private int mapperIndex;
  private long DELAY = 1000;
  private Timer timer;
  private TimerTask timerTask;

  public Reducer(String key, MasterInterface master) throws RemoteException {
    word = key;
    value = 0;
    masterNode = master;
    mapperIndex = 0;
    timer = new Timer();
    timerTask = new TimerTask() {
      @Override
      public void run() {
        pingMaster();
      }
    };
  }

  public void start() {
    timer.schedule(timerTask, 0, DELAY);
  }

  public void pingMaster() {
    try {
      MapperInterface[] mappers = masterNode.getMappers(this, mapperIndex);
      mapperIndex += mappers.length;
      for (int i = 0; i < mappers.length; i++) {
        int newCount = mappers[i].getCount(word);
        value += newCount;
      }
    } catch (Exception e) {
      System.err.println("Failed to get mappers: " + e.toString());
      e.printStackTrace();
    }
  }

  public void terminate() {
    try {
      this.masterNode.receiveOutput(word, this.value);
      System.out.println("Reduce task terminated");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    timerTask.cancel();
  }
}