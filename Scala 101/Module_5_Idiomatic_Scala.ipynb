# For expressions

After completing this lesson, you should be able to:
  - Understand the relationship between for expressions and higher order functions
  - Describe the usage of for expressions

For expressions are syntactic sugar that simplifies the work of programming a multistage transformation

Let's take an example:


```scala
val myNums = 1 to 3
myNums.map(i => (1 to i).map(j => i * j))
```

The following two examples show the same but in Java and Python.

```java
import java.util.ArrayList;

class test {
    public static void main(String[] args) {
        ArrayList<Integer> lst = new ArrayList<>();
        lst.add(1);
        lst.add(2);
        lst.add(3);

        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        ArrayList<Integer> subLst;


        for(int s=0; s < lst.size(); s++) {
            subLst = new ArrayList<>();
            int sValue = lst.get(s);

            for(int t=0; t < s+1; t++) {
                int tValue = sValue * (t + 1);
                subLst.add(tValue);
            }
            result.add(subLst);
        }

        System.out.println(result);
    }
}
```

```python
lst = range(1, 4) # python range is not inclusive

result = []
for i in lst:
    sub_result = [j * i for j in range(1, i+1)]
    result.append(sub_result)

print(result)
```

Since, we want a collections of integers rather than a collection of collections, we start the processing with `flatMap` instead of `map`.

```scala
myNums.flatMap(i => (i to i).map(j => i * j))
```

We can simplify the appearance of the code in the example above by using for-expressions. 

```scala
val myNums = (1 to 3).toVector
val result: Vector[Int] = for {
  i <- myNums
  j <- 1 to i
} yield i * j
```

The rules for using the `for` expressions is that they:
  - must with the keyword `for`
  - must have generators that use `<-`
  - the `yield` keyword will dictate whether or not a new value is returned

`for` expressions are alternative syntax (a.k.a syntactic sugar) for the combined use of `flatMap`, `map`, `withFilter` and `foreach` higher-order functions.

We can also apply guard conditions in `for` expressions. Below we modify the earlier example by filtering the original list of `myNums` to odd numbers only.

```scala
val myNums = (1 to 3).toVector
val result: Vector[Int] = for {
  i <- myNums if i % 2 == 1
  j <- 1 to i
} yield i * j
```

With this modification we have the results of starting the multiplication with the odd numbers only. You can also modify one of the other earlier examples in Java or Python to achieve the same result.

`for` expression syntax also allows for local definitions. For instance,

```scala
case class Time(hours: Int, minutes: Int)

val times = List[Time](Time(10, 10), Time(11, 11), Time(20, 9))

val result: List[String] = for {
  time <- times
  hours = time.hours if hours > 12
} yield (hours - 12) + "pm"
```

Note that this is the same as

```scala
case class Time(hours: Int, minutes: Int)

val times = List[Time](Time(10, 10), Time(11, 11), Time(20, 9))

val result: List[String] = for {
  time <- times
  if time.hours > 12
} yield (time.hours - 12) + "pm"
```

The local definition of `hours` is introduced only to simplify the code below.

All of the code written up to this point has been used to produce values. There are times when we do not want to produce a value. We are executing code to accomplish some sort of side-effect. This could be writing to a database or printing something to the screen or any operation that doesn't return a value but has an external effect. `for` expressions accommodate this situation by allowing us to omit the use of `yield`. 

```scala
for (n <- 1 to 3) println(n)
```

The code above is equivalent to 

```scala
(1 to 3).foreach(i => println(i))
```

To summarize:

  - `for` expressions are a more readable way of expressions consisting of nested `map`, `flatMap`, `filter` and `foreach`.
  - the compiler will translate for expressions that we write into a chain of `map`, `flatMap`, `filter` and `foreach`
  - the syntax allows for filtering and local definitions
  - use `for` expressions whenever you are iterating thought more than one collection-like object or want to chain multiple transformations


# Patterns Matching


Pattern matching is a powerful construct that improves on the familiar `switch`/`case` syntax (which is absent in Scala). It is best to start with a simple example:

```scala

val result: Int = 10

result match {
  case 1 => println("found 1")
  case someInteger: Int => println("found some value of type Int")
  // the compiler will issue a warning because this code is acutally impossible to reach. Any value of type Int will match the second condition
  case _ => println("found something that is not 1 or some other integer")
}
```

Matching is started with the keyword `match` which is followed by a block of case statements. The left-hand side of the `=>` in the `case` statement supports several varieties of syntax. The right-hand side can contain arbitrary code.

In the first `case` statement, we've checking if the `result` we're trying to find a match for is an integer 1. If it is not, the pattern matching moves on to the next `case` statement. In the following statement, if the value of `result` has a type of `Int`. If that is true, the value of `result` is stored in the name `someInteger`, this is known as a `type` pattern. If this pattern match is not successful, ie if `result` is not of type `Int`, the next case is evaluated. This last statement is a catch-all that will successfully match anything. When it matches, the string `"found something that is not 1 or some other integer"` will be printed to the screen.
We can wrap this pattern matching expressions into a function and experiment with it.

```scala
def isOneOrInteger(value: Int): Unit =
  value match {
    case 1 => println("found 1")
    case someInteger: Int => println("found some value of type Int")
    case _ => println("found something that is not 1 or some other integer")
  }

isOneOrInteger(1)
isOneOrInteger(22)
```

With this definition of the function the compiler will not actually allow us to pass anything other than integers as parameters to the function. In order to get around this, we have to relax the type of `value` to `Any`. Make this change and pass parameters of other types into the function. Observe the output.

Pattern matching is very useful when you want to act on specific values of some case class or a hierarchy of types that represents a set of alternatives (also known as an Algebraic Data Type).

As a reminder, an Algebraic Data Type is a way to encode the fact that a certain type is represented by a finite set of alternatives. For instance, in Scala the Boolean type contains two values, `true` and `false`. We could represent these as distinct types. This is shown below:

```scala
sealed trait MyBoolean {
  def value: Boolean
}
case class True(value: Boolean = true) extends MyBoolean
case class False(value: Boolean = false) extends MyBoolean
```

This the typical representation of an ADT in Scala. Closing the `trait` with `sealed` means that all of the definitions that are inherit from this trait are are defined in the same Scala file where this definition is located.


As another example, we could model the behavior of a light switch with an ADT. This is shown below:

```scala
sealed trait LightSwitch
case object On extends LightSwitch
case object Off extends LightSwitch
```

Since we've notified the compiler, by using `sealed`, that all of the subtypes of `LightSwitch` are in this file, it (the compiler) will verify that we have covered all of the cases when we pattern match on a value of type `LightSwitch`. Assign a value to `light` and observe the behavior.

```scala
sealed trait LightSwitch
case object On extends LightSwitch
case object Off extends LightSwitch

val light: LightSwitch = Off

def status(value: LightSwitch): Unit = 
  value match {
    case On => println("the switch is on")
    case Off => println("this switch is off")
  }

status(light)
```

We can write something similar to the above for our custom `Boolean` type. These values are little bit more complex in that they actually carry some values. However, using pattern matching we can de-compose or `unapply` case class into its components. Set value of `b` to one of the possible alternatives and observe the difference.

```scala
sealed trait MyBoolean {
  def value: Boolean
}

case class True(override val value: Boolean = true) extends MyBoolean
case class False(override val value: Boolean = false) extends MyBoolean


val b: MyBoolean = False()

def isItActuallyTrue(value: MyBoolean): Unit =
  value match {
    case True(true) => println("it is actually true!")
    case True(false) => println("it is pretending to be true!")
    case False(true) => println("it is pretending to be false, but is actually true!")
    case False(false) => println("it is really false!")
  }
```


# Options

`Option` represents a presence or an absence of a value. Consider division. We could implement division and handle division by 0 by throwing an exception like so:

```scala

def divE(numerator: Int, denominator: Int): Int =
  if (denominator == 0) throw new Exception("Denominator is 0") else numerator / denominator

divE(10, 2)
divE(2, 0)
```

However, this is far from idiomatic Scala. In the above we interrupting the flow of the program with an exception that we can easily handle ourselves. Furthermore, there is nothing about the function definition that communicates to us that this function may not produce a value in some cases. The following is a better definition of the same function:

```scala
def divO(numerator: Int, denominator: Int): Option[Int] =
  if (denominator == 0) None else Some(numerator / denominator)

divO(10, 2)
divO(2, 0)
```

Pattern matching makes processing values of type `Option` straightforward.

```scala
def stringLegnth(value: Option[String]): Int =
  value match {
    case Some(str) => str.length
    case None => 0
  }
```

This pattern, where we want to extract the value from an `Option` or provide a default value when the it is None occurs frequently. It is more concisely represented by the function `getOrElse`. Thus, we can reimplement the functionality of `stringLegnth` as: 

```scala
def stringLegnth(value: Option[String]): Int =
  value.getOrElse("").length
```

We provide the default value of `""` (empty string) for the case when `value` is `None`. 

There are a number of higher order functions we can use with `Option`. For example, another way to implement `stringLegnth` to combine `map` and `getOrElse`.

```scala
def stringLegnth(value: Option[String]): Int =
  value.map(str => str.length).getOrElse(0)
```

If `value` is a `Some` we will compute the length of the string that it contains. If it is `None`, `map` will not do anything. Subsequently, `getOrElse` will be called. Note, that `getOrElse` is now being called on an `Option[Int]` value, because by using `map` we have changed the type of the value inside the `Option` from `String` to `Int`. As always, `getOrElse` will either return the contents of the `Option` or the default value of 0.

We can sequence processing of options with `flatMap`. For example,

```scala
def fullName(firstName: Option[String], lastName: Option[String]): Option[String] =
  firstName.flatMap(fn => lastName.map(ln => s"$fn $ln"))

val bothOption: Option[String] = fullName(Option("John"), Option("Doe"))
val oneOption: Option[String] = fullName(Option("Prince"), None)
```

As you may remember the section on `for` expressions, nested combinations of `map` and `flatMap` can be re-written using `for` expressions. Thus, the `fullName` function above can be written as:

```scala
def fullName(firstName: Option[String], lastName: Option[String]): Option[String] =
  for {
    fn <- firstName
    ln <- lastName
  } yield s"$fn $ln"

fullName(Option("John"), Option("Doe"))
fullName(Option("Prince"), None)
```

# Handling Failure

There are a number of mechanisms that we can use in Scala to handle failures. In particular, we have already seen that instead of throwing exceptions we can avoid them by using `Option`. The Scala standard library provides another abstraction to help us with code where we have to handle an exception rather than avoiding it, `Try`.

`Try` is actually an ADT that consists of `Success` and `Failure`. `Success` captures the result of the successful computation. `Failure` captures the exception that occurred during the computation. This is shown below.

```scala
import scala.util.{Try, Success, Failure}

def divT(numerator: Int, denominator: Int): Try[Int] =
  Try(numerator / denominator)

val result: Try[Int] = divT(10, 0)

result match {
  case Success(v) => println("got the result of: " + v)
  case Failure(e) => println("got an exception of: " + e)
}
```

As can be seen above, we can use pattern matching to process a value of type `Try`. Similar to `Option`, `Try` implements a number of higher order functions. Some of these functions are shown below.

```scala
import scala.util.{Try, Success, Failure}

def divT(numerator: Int, denominator: Int): Try[Int] =
  Try(numerator / denominator)

val result: Try[Int] = divT(10, 0)

// getOrElse on Try works the same was as on Option. note that the value has to be the same type as the success case
val getOrElseValue: Int = result.getOrElse(0)

// similar to Option, if `result` is a `Success` add 10 to it. Otherwise, do nothing
val mapValue: Try[Int] = result.map(success => success + 10)

// just like with `Option` we can sequence try computations with `flatMap`
val flatMapValue: Try[Int] = result.flatMap(success => divT(success, 10))

// transform takes two functions as parameters, one for each type of result
val transformValue: Try[Int] = result.transform(s => Success(s + 10), ex => Success(0))
```

Since we have `map` and `flatMap` implemented on `Try`, we can use `for` expressions with `Try`.


```scala
import scala.util.{Try, Success, Failure}

def divT(numerator: Int, denominator: Int): Try[Int] =
  Try(numerator / denominator)

val result: Try[Int] = for {
  r1 <- divT(10, 10)
  r2 <- divT(1100, 34)
} yield r1 * r2
```

The above can also be represented as:

```scala
import scala.util.{Try, Success, Failure}

def divT(numerator: Int, denominator: Int): Try[Int] =
  Try(numerator / denominator)

val resultForExpression: Try[Int] = for {
  r1 <- divT(10, 10)
  r2 <- divT(1100, 34)
} yield r1 * r2

val resultFlatMap = divT(10, 10).flatMap(r1 => divT(1100, 34).map(r2 => r1 * r2))
```

# Futures

By relying on immutability Scala makes multi-threaded programming easier. In addition to this, the Scala standard library provides an additional abstract to represent computations that may happen at a later time or on another thread. This abstraction is represented by the `Future` type.

Prior to being able to create values of type `Future` we must specify on what thread pool the execution will be performed. The framework you're working with (like Play or Akka) will typically provide a way for you to use its own thread pool. In Scala, Java thread pools as wrapped into values of type `ExecutionContext`. Creating an `ExecutionContext` is shown below:

```scala
import scala.concurrent.ExecutionContext
import java.util.concurrent.ForkJoinPool

val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())
```

with this value in scope we can start creating values of type `Future`.

```scala
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.ForkJoinPool

val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

// use `.successful` if simply wrapping a static value into a `Future`. This doesn't create threads
val f1: Future[Int] = Future.successful(1)

def longComputation(duration: Int): Int = {
  // pretend as if time consuming work is being done
  import java.lang.Thread.sleep
  sleep(duration)
  duration
}

// this will be executed in a thread on the ExecutionContext
// we are passing the ExecutionContext value explicitly
val f2: Future[Int] = Future(longComputation(100))(ec)
```

Passing the `ExecutionContext` value every time we use a `Future` is error-prone and hards readability. `Future` instances can take this parameter as an implicit parameter. Thus, we can write,

```scala
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.ForkJoinPool

implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

def longComputation(duration: Int): Future[Int] = {
  // pretend as if time consuming work is being done
  import java.lang.Thread.sleep
  Future {
    sleep(duration)
    duration
  }
}

val f3: Future[Int] = longComputation(100)
```

Note that when we run code like `Futuer(longComputation(100))`, the result that is returned an `Future` that has not yet been completed. Eventually, the value will be computed and available. However, this doesn't prevent as from writing code that will be executed after the value as been fulfilled. 

For example, we can pattern match on `Future`.

```
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import java.util.concurrent.ForkJoinPool

implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

def longComputation(duration: Int): Future[Int] = {
  // pretend as if time consuming work is being done
  import java.lang.Thread.sleep
  Future {
    sleep(duration)
    duration
  }
}

val f4: Future[String] = Future {
  import java.lang.Thread.sleep
  sleep(100)
  "hello " + "world!"
}

f4.onComplete {
  case Success(value) => println("!" + value)
  case Failure(ex) => println("something went wrong. we took too long to add two strings!")
}
```

We can use familiar higher order functions and `for` expressions.


```scala
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import java.util.concurrent.ForkJoinPool

implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

def longComputation(duration: Int): Future[Int] = {
  // pretend as if time consuming work is being done
  import java.lang.Thread.sleep
  Future {
    sleep(duration)
    duration
  }
}

val f4: Future[String] = Future {
  import java.lang.Thread.sleep
  sleep(100)
  "hello " + "world!"
}

// use the result if the Future was successful, otherwise, do nothing.
val f6: Future[Int] = longComputation(100).map(successfulResult => successfulResult + 100)
val f7: Future[String] = longComputation(100).flatMap(r1 => longComputation(100).map(r2 => r1.toString + r2.toString))

val f8: Future[String] = for {
  r1 <- longComputation(100)
  r2 <- longComputation(100)
} yield (r1.toString + r2.toString)
```

There are many additional methods on `Future` that can be helpful. For example, `recover` allows us to specify how to handle specific types of exceptions. This is similar to `onComplete` but we do not have to specify the success case.

```
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import java.util.concurrent.ForkJoinPool

implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

def longComputation(duration: Int): Future[Int] = Future {
  // pretend as if time consuming work is being done
  import java.lang.Thread.sleep
  sleep(duration)
  throw new IllegalStateException
}

longComputation(100).recover {
  case _: IllegalStateException => 0
}
```

There are many other functions and details to the workings of `Future`. The Scala documentation provides a great overview of most of the features and additional functionality. You can find those details here: [https://docs.scala-lang.org/overviews/core/futures.html](https://docs.scala-lang.org/overviews/core/futures.html)
