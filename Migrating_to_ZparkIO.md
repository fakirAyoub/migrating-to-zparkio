# Migrating from a plain Spark Application to ZIO with ZparkIO

In this article, we'll see how you can migrate your Spark Application into [ZIO](https://zio.dev) and [ZparkIO](https://github.com/leobenkel/ZparkIO) so you can benefit from all the wonderful features that ZIO offers and that we'll be discussing.

## What is ZIO?

ZIO is defined, according to official documentation as **a library for asynchronous and concurrent programming that is based on pure functional programming.** In other words, ZIO helps us write code with type-safe, composable and easily testable code, all by using safe and side-effect-free code.

**ZIO is a data type**. Its signature, *ZIO[R, E, A]* shows us that it has three parameters:

1. **R**, which is the environment type. Meaning that the effect that we're describing needs an environment, which is optional (would be `Any` when optional). The environment describes what is required to execute this Task.
2. **E**, which is the error with which the effect *may* fail.
3. **A**, which is the data type that is returned in case of success.

## How do we apply ZIO to Spark?

From the signature of the **ZIO** data type, we can deduce that in order for us to use Spark with ZIO, we need to specify one necessary parameter, namely, the **R**. In our case, the **R** is the **SparkSession**, which is the entry point to every Spark Job, and the component that resides in the **Spark Driver**.

The process of integrating ZIO to our Spark Programs can be hard, especially for beginners. Thankfully, [Leo Benkel](https://leobenkel.com) has created a library, namely **[ZparkIO](https://github.com/leobenkel/ZparkIO)**, which is a library project that implements the whole process of marrying Spark and ZIO together! 

## Using ZparkIO to bootstrap a Spark / ZIO project

The first step is to create a trait that extends **ZparkioApp[R, E, A]**, where you would need to override two methods: **makeCli** and **runApp**.

**makeCli(args: List[String])** compiles all the program arguments for you (for the moment, *scallop* is used by default, but we're in the process of extracting this module so you can use your CLI tool of choice). 

**runApp():ZIO[COMPLETE_ENV, E, A]** is the main function where your program's logic resides.

```scala
override def runApp(): ZIO[COMPLETE_ENV, Throwable, Unit] = ???
```

Once you're setup, your SparkSession can be access as follows:

```scala
for {
  spark <- SparkModule()
}
```

And if you want to acces the implicits of Spark, the trick is to map over SparkModule():

```scala
for {
  spark <- SparkModule().map { ss => 
      import ss.implicits._ 
      ???
  }
}
```

## Migrating your project

Once your project is setup, the process of migrating your project can be pretty straightforward.

### Defining your program's arguments

ZparkIO uses **[scallop](https://github.com/scallop/scallop)**. So to define the CLI arguments of your program you would need to define a case class, *Arguments*, which has the following signature:

```scala
case class Arguments(input: List[String])
  extends ScallopConf(input) with CommandLineArguments.Service { ??? }
```

Then, each one of your arguments has to be declared as follows:

```scala
  val argumentOne: ScallopOption[String] = opt[String](
    default = None,
    required = true,
    noshort = true,
    descr = "Description of this argument"
  )
```

HINT: The argument defined here is called "argumentOne", however, it has to be called as **argument-one** from the CLI while executing your project. Scallop logic! :D

### Migrating your code

The first step is that **every function / helper that you have in your program needs to start returning something of type ZIO**. As an example, let's take two methods, one that reads data from an external file system without ZparkIO, and another that uses it:

```scala
def readData[A](
    inputPath: String
  )(
    implicit sparkSession: SparkSession
  ): Dataset[A] = 
    sparkSession.read.parquet(inputPath)
```

In a plain Spark application, this method returns a Dataset of some type A, and... That's it! If we fail to read the inputPath given as a parameter for some reason, the whole program crashes and we do not catch the reason why (at least not at a first sight).

This same method, using ZIO and ZparkIO will be written as follows:

```scala
def readData[A](inputPath: String): ZIO[SparkModule, Throwable, Dataset[A]] = 
  for {
    spark   <- SparkModule()
    dataset <- Task(spark.read.parquet(inputPath))
  } yield dataset
```

Please note that we didn't have to use an implicit parameter as the session is already provided by the SparkModule() method of the library. The function **readData** uses a Spark Environment, namely **SparkModule**, may fail with a Throwable, and in case of success, returns a Dataset of some type A (I'm using Datasets here instead of DataFrames because first, we need some better typing, and second, Leo is allergic to Dataframes).

The instruction to read data from the filesystem is wrapped into a *Task*. Task is of type IO[Throwable, A], which means that it does not depend on any environment (implicitely *Any*). Then we provide the dataset we just read, which matches the return type of our function.

Once, your function is wrapped in a Task as such, you can start leveraging ZIO features such as `retry` and `timeout`.

```scala
protected def retryPolicy =
  Schedule.recurs(3) && Schedule.exponential(Duration.apply(2, TimeUnit.SECONDS))
    
def readData[A](inputPath: String): ZIO[SparkModule, Throwable, Dataset[A]] = 
  for {
    spark   <- SparkModule()
    dataset <- Task(spark.read.parquet(inputPath))
      .retry(retryPolicy)
      .timeoutFail(ZparkioApplicationTimeoutException())(Duration(10, TimeUnit.MINUTES))
  } yield dataset
```

In this example, if the `read` function fails, it will retry up to 3 times with an exponential wait in between each retries but the total amount of time spent on this task cannot exceed 10 minutes.

### Some other function examples:

Obviously, your Spark program has more methods than just reading data from HDFS or S3. Let's take another example:

```scala
def calculateSomeAggregations[A](ds: Dataset[A]): Dataset[A] = {
  ds
  .groupBy("someColumn")
  .agg(
  	sum(when(col("someOtherColumn")) === "value", lit(1)).otherwise(lit(0))
  )
}
```

This function calculates some aggregations of some Dataset of type A. One easy way to begin your migration to ZIO and ZparkIO is to wrap its calculations into a *Task*: 

```scala
def calculateSomeAggregations[A](ds: Dataset[A]): IO[Throwable, Dataset[A]] = Task {
  ds
  .groupBy("someColumn")
  .agg(
  	sum(when(col("someOtherColumn")) === "value", lit(1)).otherwise(lit(0))
  )
}
```

And... Voilà ! Your safe method is ready!

## But after all, why would you need to migrate your project and start using ZIO?

Well, one first and obvious argument is that it's... safer, more composable and lets you more easily think about the logic of your program.

Alright, now let's take the following example:

```scala
val df1 = spark.read.parquet("path1")
val df2 = spark.read.parquet("path2")
val df3 = spark.read.parquet("path3")
...
val dfn = spark.read.parquet("pathn")
```

We all know that Spark is the leading framework for distributed programming and parallel calculations. However, imagine that you are running a Spark Program in an EMR cluster having 20 nodes of 32GB of RAM and 20 CPU Cores each. That's a lot of executors to be instanciated in. You setup a cluster of this size because you do some heavy **joins**, **sorts** and **groupBys**. But still, you read a lot of parquet partitions in the beginning of your program, and the problem is that... All the instructions shown earlier will be executed sequentially. When `df1` is being read, it certainly does not use the whole capacity of your cluster, and the next instruction needs to wait for the first one to end, and so on. That's a lot of resources waste.

Thanks to **ZIO** and its **Fibers** feature, we can force those readings to be run in parallel as follows:

```scala
for {
  df1 <- Task(spark.read.parquet("path1")).fork
  ...
  dfn <- Task(spark.read.parquet("pathn")).fork
  
  readDf1 <- df1.join
  readDf2 <- df2.join
}
```

The **fork** / **join** combo will force those instructions to be executed in parallel, and thus, a minimum of the Cluster's resources waste!

Another useful method that we can use in this context is **foreachPar**. Let's suppose that we have a list of paths of parquet partitions that we want to read, in parallel using our **readData(inputPath: String)** method defined earlier:

```scala
for {
  dfs <- ZIO.foreachPar(filePaths) {
    filePath => {
      readData(filePath)
    }
  }
} yield dfs
```

The value that we return here is a List of the Dataframes that we read, that we can now use throughout our program.

And even further, `foreachParN` which allow you to specify how many task can run in parallele at most:

```scala
for {
  dfs <- ZIO.foreachParN(5)(filePaths) {
    filePath => {
      readData(filePath)
    }
  }
} yield dfs
```

This code will limit the parallel execution to `5`, never more. Which is a great way to manage your resources. This could even be a multiple of the quantity of executors available in the cluster.

### Wrap up

We hope that his article will give you a taste about why you should use ZIO and Spark with the help of ZparkIO to get your Spark jobs to the next level of performance, safety and fun!

