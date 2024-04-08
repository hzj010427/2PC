# 2PC
This provides the classes needed for Project 4.

The main classes (Project4, ProjectLib) and the sample are in the lib 
directory.  

A skeleton Server and UserNode are provided in the sample directory.  
These do not actually do anything other than intialize ProjectLib and 
demonstrate how to send and receive messages.  To build these, ensure 
your CLASSPATH has the absolute paths to the lib and sample directory 
included.  Then run make in the sample directory.  

We have provided some files to start testing your code. 
A set of sample images and test scripts are provided in test.tar.  
To use these, untar the file to produce a test directory with a set
of subdirectories.  Change your woking directory to "test".  
Then, from this directory, run Project4, e.g.:
	java Project4 15440 scripts/1-simple-commits.txt
The Server will run in the Server directory, and up to 4 UserNodes in 
a, b, c, and d.  Ensure that the committed composite images are 
generated in the Server directory, and that the corresponding 
sources are removed from the UserNode directories.  There are
two additional scripts provided as well.  

To run the test again, it is simply best to completely remove the 
test directory and recreate it from test.tar.  This will ensure 
that all of the directories are in a clean state.

See the handout for more details.