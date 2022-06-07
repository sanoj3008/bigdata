## Big Data - Weekly Assignments
This submission can also be inspected by using my public git repository [sanoj3008/bigdata](https://github.com/sanoj3008/bigdata). 
It contains all relevant files for this and previous submissions.
### Team member
It is only me: **jonasschell@uni-koblenz.de**

### Project structure
All solutions can be found under `src/main/scala/org.softlang.bigdata`.
Each Assignment has its own package **assX**.

#### Assignment 1 (.ass1)
Mutable solution is implemented from line 57. Immutable solution comes behind that.

#### Assignment 2 (.ass2)
The solution is implemented in *Assignment2.scala* and fully commented.
The method `execution(nTasks: Int): Int`  contains the main algorithm. For the needed evaluation in exercise 2, the algorithm is called by the additional method `ccRealisation(nTasks: Int): mutable.Map[Int, Int]`

In the *main*-method you can find several *ccRealisation* calls for the different parameter setups. The result of each execution will be stored in `res/ass2/FILENAME.csv`.

A .pdf file with the value evaluation can also be found in `res/ass2/evaluation.pdf.`

### Assignment 3 (.ass3)
The solution is implemented in Tasks.scala. All test cases are successfully executed.

Just one addition: For task 3 and 5 I provided two possible solutions. I don't know, if the improved one is permissible.

### Assignment 4 (.ass4)
The solution is implemented in the provided scala files.

Exercise 1 writes its solution into the specified file. To run in your environment, just change the value of `PATH_TO_CSV`. The whole implementation if fully commented.

Exercise 2 has the same structure as in Assignment 3. All test cases are successfully executed.