package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface MasterInterface extends Remote {
  public MapperInterface[] getMappers(ReducerInterface reducer, int index) throws RemoteException, AlreadyBoundException;
  public void markMapperDone(MapperInterface mapper, String[] keys) throws RemoteException;
  public void receiveOutput(String key, int value) throws RemoteException;
}