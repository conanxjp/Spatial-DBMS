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

sdotest: SDOTest
	$(JAVA) tests.SDOTest





