# modify the paths appropriately
JDKPATH = /Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test the sqlinterface

SDOTest: SDOTest.java
	$(JAVAC) TestDriver.java SDOTest.java
	
Part3WriteTest: Part3WriteTest.java
	$(JAVAC) TestDriver.java Part3WriteTest.java
	
Part3ReadTest: Part3ReadTest.java
	$(JAVAC) TestDriver.java Part3ReadTest.java

sdotest: SDOTest
	$(JAVA) tests.SDOTest
	
write: Part3WriteTest
	$(JAVA) tests.Part3WriteTest
	
read: Part3ReadTest
	$(JAVA) tests.Part3ReadTest




