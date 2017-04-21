package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface ReducerInterface extends Remote {
  public void start() throws RemoteException;
  public void terminate() throws RemoteException;
}