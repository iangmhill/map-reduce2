package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface MapperInterface extends Remote {
  public void processInput(String input, MasterInterface master) throws RemoteException, AlreadyBoundException;
  public int getCount(String key) throws RemoteException;
}