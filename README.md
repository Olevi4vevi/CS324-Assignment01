# CS324_Assignment01
Java implementation for the CS324 Assignment 1 project. 

--> Requirements <--:
JDK 17+ (java -version to check)
Maven (mvn -version to check)

--> Setup <--:
Open a terminal in the CS324-Assignment01 folder (same folder as pom.xml).
Check pom.xml — the <maven.compiler.release> number should match your Java version. Edit it if it doesn't match.

Build:
   mvn clean compile

Confirm it says BUILD SUCCESS.

--> Running it <--:

Open a separate terminal for each of these and leave them all running.

1. Bootstrap (start first):

java -cp target/classes Bootstrap.BootstrapNode

2. Workers (start 2 or more, a few seconds apart):

java -cp target/classes Worker.WorkerNode

Wait about 10-15 seconds after the last one — all workers should end up agreeing on the same coordinator.

3. Client:

java -cp target/classes Client.ClientGUI

Click Connect, pick a job type, enter numbers (or load a CSV), and click Submit Job.

--> If something breaks <--
1. Build fails over "release version not supported" → your JDK doesn't match the number in pom.xml. Fix the number and rebuild.
2. "Could not find or load main class" → the build didn't actually succeed, or you're in the wrong folder. Check you're inside CS324-Assignment01 and that target/classes exists.
3. A worker never shows a coordinator → start Bootstrap and all workers fresh, close together in time, and give it ~15 seconds.
