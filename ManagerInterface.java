package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface ManagerInterface extends Remote {
  public ReducerInterface createReduceTask(String key, iMaster master) throws RemoteException, AlreadyBoundException;
  public MapperInterface createMapTask() throws RemoteException, AlreadyBoundException;
}