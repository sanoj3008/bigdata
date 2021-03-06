## Big Data - Weekly Assignments
This submission can also be inspected by using my public git repository [sanoj3008/bigdata](https://github.com/sanoj3008/bigdata). 
It contains all relevant files for this and previous submissions.
### Team member
It is only me: **jonasschell@uni-koblenz.de**

### Project structure
All solutions can be found under `src/main/scala/org.softlang.bigdata`.
Each Assignment has its own package **assX**. In each assignment package besides the tasks files you can also find the solutions there in the **sol** package. 

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

### Assignment 5 (.ass5)
The solution is implemented in the provided scala files.

For exercise 1 I didn't find any solutions in case of *myReduceByKey* and *myJoin*. I spend a lot of time, but I didn't get it. :smirk:
In case of *myReduce* I tried something out, but I don't know, if this procedure is legal.

Exercise 2 is fully implemented and commented in *Assignment52Stub*.
**WARNING:** The required movies.csv must be externally provided in *res/ass5* before execution.

### Assignment 6 (.ass6)
The solution is implemented in the provided scala file.

### Assignement 7 (.ass7)
The solution is implemented in the provided scala file. The necessary functions `def get(key: String): Option[String]` and `def set(key: String, value: String): Unit` are fully commented.

For my solution I seperate the set of machines in multiple chunks:

![chunks](https://github.com/sanoj3008/bigdata/blob/main/res/ass7/chunks.png)

If you want to add for example *key_23* into the key-value-store you have first to choose one of the chunks randomly. After that you calculate the position of the data by using the modulo operator on the absolute hashcode.

If you now want to check, whether the requested key has a value in our data system, you just need to look in each of the chunks at the specified position for the corresponding key value pair.

A .pdf file with the needed execution analysis is provided in `./res/ass7`.

### Assignment 8 (.ass8)
The solution is implemented in the provided scala file. All consumer functions are fully commented.

For a successful execution of this exercise you need to refer to the *frank.txt* of assignment 4.