## Big Data - Weekly Assignments
### Team member
It is only me: <b>jonasschell@uni-koblenz.de</b>

### Project structure
All solutions can be found under src/main/scala/org.softlang.bigdata.
Each Assignment has its own package assX.

#### Assignment 1 (.ass1)
Mutable solution is implemented from line 57. Immutable solution comes behind that.

#### Assignment 2 (.ass2)
The solution is only implemeted in Assignment2.scala and fully commented.
The method ```execution(nTasks: Int): Int```  contains the main algorithm. For the needed evaluation in exercise 2, the algorithm is called in an additional method called ```ccRealisation(nTasks: Int): mutable.Map[Int, Int]```

In the <i>main</i>-method you can find several <i>ccRealisation</i> calls for the different parameter setups. The result of each execution will be stored in `res/ass2/FILENAME.csv`.

A .pdf file with the value evaluation can also be found in `res/ass2/evaluation.pdf.`