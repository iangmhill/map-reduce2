javac -d . MasterInterface.java ManagerInterface.java MapperInterface.java ReducerInterface.java Master.java Manager.java Mapper.java Reducer.java
java -classpath . -Djava.rmi.server.codebase=file:./ hadoop.Master 8000 127.0.1.1:8001