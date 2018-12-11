/*....*/

// Project 3
// Distributed Operating System
// Fall 2015
// Chiranjib Sur, Pramit Dutta
// Chord Protocol

import java.security.MessageDigest

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scala.math.pow
import scala.util.Random
import java.lang.Math.abs
import scala.math._
import scala.collection.immutable.List
import scala.util.control.Breaks._

case class startActorAgent(index:Int, node:Int, nodesRefered:List[ActorRef], nodeList:List[Int], dataIndex:List[Int], pred:Int, succ:Int, request:Int, m:Int) // starting the actor models
case class createFingerTable()
case class createDataTable()
case class searchRoute(nodeID:Int, item:Int, count:Int)
case class closeProgam()
case class countHop(hops:Int, success:Int)



object Project3 extends App {
  var noOfNodes : Int = args(0).toInt  // number of nodes
  var noOfRequests : Int = args(1).toInt  // number of nodes
  print(noOfNodes+" "+noOfRequests+" ")
  //var topology : String = args(1).toString // topology
  //var algorithm : String = args(2).toString // algorithm kind
  //print(noOfNodes+" "+topology+" "+algorithm+" ")  // testing the arguments

  print("\n=================================================================\n")
  print("Starting the Simulation  ")
  print("\n=================================================================\n")
  print(" with "+noOfNodes+" number of nodes"+" and "+noOfRequests+" number of requests.")
  print("\n=================================================================\n")

  // stating the main system to create topology and create the actors for the further processing of the algorithm
  val system=ActorSystem("ChordProtocol") // this is a standard way of declaration
  val masterActor =system.actorOf(Props(new Master(noOfNodes,noOfRequests)),"master") // this is also a standard way of declarartion
  // Switching the control to master
}

class Master(noOfNodes: Int, noOfRequests: Int) extends Actor {
  var no_of_nodes: Int = noOfNodes
  //
  var no_of_req: Int = noOfRequests //

  var no_of_data: Int = 10

  //var fingerList: List[Int] = Nil
  //var dataList: List[Int] = Nil
  var pred: Int = 0
  var succ: Int = 0

  var m: Int = 9 //
  var sentcount: Int = 0
  var countg:Int = 0
  var counthop:Float = 0
  var countSucess:Float = 0
  var countFailure:Float = 0
  var Averagehop:Float = 0
  var SuccessRatio:Float = 0

  var nodesRefered: List[ActorRef] = Nil
  // reference to nodes or the actor, initialized to nothing
  var nodesIndex: List[Int] = Nil
  // position value of node in nodesRefered
  var dataIndex: List[Int] = Nil

  // determination of m
  if (no_of_nodes <= pow(2,m)) {
    m = 9
  }
  else {
    //println("There may occur a overflow of Integer, so cannot be handled") // failed test case
    m = 9
    while (no_of_nodes > pow(2,m)) {
      m = m + 1
    }
  }


  //Creating Nodes as actor variables
  for (i <- 0 to (pow(2,m).toInt - 1)) { // starting from zero and hence minus one
    nodesRefered ::= context.actorOf(Props(new Node))  // each of the reference to nodes are assigned some value
  }
  print("\n"+" Nodes Created\n") // checkpointing the program // for just testing

  // generation of nodes and placing them in ring xz
  for (i <- 0 to (no_of_nodes - 1)) {
    // generate address or name and convert to SHA 1 hash
    var randomStr:StringBuilder = new StringBuilder()
    var NodeID:String = (randomStr.append(Random.alphanumeric.take(20).mkString)).toString()

    val sha_ID = MessageDigest.getInstance("SHA-1")
    sha_ID.update(NodeID.getBytes("UTF-8"))
    val digest_ID = sha_ID.digest()
    val hash_ID = new StringBuffer()

    for (j <- 0 to digest_ID.length - 1) {
      val hex_ID = Integer.toHexString(0xff & digest_ID(j))
      if (hex_ID.length() == 1) hash_ID.append('0')
      hash_ID.append(hex_ID)
    }
    //println("Node ID: "+(i+1)+" "+NodeID+" Hash: "+hash_ID) // testing

    val hashID:String = hash_ID.toString()
    //print("\n"+hashID(1))

    // declare temp1
    //val hashID1:String = "0000011000000000000000000000000000000000" // test case
    //println("\n"+hashID1(0)) // testing
    //if (hashID1(0) == 'a') println("hi") // testing
    var m1:Int = 7
    var temp1: Int = 0
    var sum: Long = 0
    for (i <- 0+33 to m1-1+33) {
      if (hashID(i) == 'a') {
        temp1 = 10
      }
      else if (hashID(i) == 'b') {
        temp1 = 11
      }
      else if (hashID(i) == 'c') {
        temp1 = 12
      }
      else if (hashID(i) == 'd') {
        temp1 = 13
      }
      else if (hashID(i) == 'e') {
        temp1 = 14
      }
      else if (hashID(i) == 'f') {
        temp1 = 15
      }
      else {
        temp1 = hashID(i).toInt - 48
      }
      //print("\n"+temp1) // test case
      sum = sum + pow(16,(m1-1-i+33)).toInt * temp1
    }

    // print("\nSum : "+sum) // testing

    // mod 2^m
    sum = sum%((pow(2,m)).toInt)
    //print("\nSum%mod : "+sum)

    nodesIndex ::= sum.toInt
  }
  //println(nodesIndex) // testing
  // sort the list nodesIndex
  nodesIndex = nodesIndex.sorted
  println("nodesIndex"+nodesIndex) // testing

  //println(m) // testing

  // for data

  for (i <- 0 to (no_of_data) - 1) {

    // generate name and convert to SHA 1 hash
    var randomStr:StringBuilder = new StringBuilder()
    var NodeID:String = (randomStr.append(Random.alphanumeric.take(20).mkString)).toString()

    val sha_ID = MessageDigest.getInstance("SHA-1")
    sha_ID.update(NodeID.getBytes("UTF-8"))
    val digest_ID = sha_ID.digest()
    val hash_ID = new StringBuffer()

    for (j <- 0 to digest_ID.length - 1) {
      val hex_ID = Integer.toHexString(0xff & digest_ID(j))
      if (hex_ID.length() == 1) hash_ID.append('0')
      hash_ID.append(hex_ID)
    }
    //println("Node ID: "+(i+1)+" "+NodeID+" Hash: "+hash_ID) // testing

    val hashID:String = hash_ID.toString
    //print("\n"+hashID(1))

    // declare temp1
    //val hashID1:String = "0000011000000000000000000000000000000000" // test case
    //println("\n"+hashID1(0)) // testing
    //if (hashID1(0) == 'a') println("hi") // testing
    var m1:Int = 7
    var temp1: Int = 0
    var sum: Long = 0
    for (i <- 0+33 to m1-1+33) {
      if (hashID(i) == 'a') {
        temp1 = 10
      }
      else if (hashID(i) == 'b') {
        temp1 = 11
      }
      else if (hashID(i) == 'c') {
        temp1 = 12
      }
      else if (hashID(i) == 'd') {
        temp1 = 13
      }
      else if (hashID(i) == 'e') {
        temp1 = 14
      }
      else if (hashID(i) == 'f') {
        temp1 = 15
      }
      else {
        temp1 = hashID(i).toInt - 48
      }
      //print("\n"+temp1) // test case
      sum = sum + pow(16,m1-1-i+33).toInt * temp1
    }

    // print("\nSum : "+sum) // testing

    // mod 2^m
    sum = sum%(pow(2,m).toInt)
    //print("\nSum%mod : "+sum)

    dataIndex ::= sum.toInt
  }

  // sort the list dataIndex
  dataIndex = dataIndex.sorted

  println("dataIndex "+dataIndex)

  // for each element in nodesIndex, generate a finger table, predecessor and initialize data table
  for (i <- 0 to (no_of_nodes - 1)) {
    // calculate pred and succ
    if (countg == 0) {
      pred = nodesIndex(nodesIndex.length - 1)
      succ = nodesIndex(countg + 1)
    }
    else if (countg == nodesIndex.length - 1) {
      pred = nodesIndex(countg - 1)
      succ = nodesIndex(0)
    }
    else {
      pred = nodesIndex(countg - 1)
      succ = nodesIndex(countg + 1)
    }
    joinNode()
  }


  //failure model implementation
  //if (Random.nextInt(no_of_nodes) > Random.nextInt(no_of_nodes)) {
  for (i112 <- 0 to round(no_of_data*20/100)) {
    var random_node = nodesIndex(Random.nextInt(nodesIndex.length))
    println("hoho"+random_node)
    context.stop(nodesRefered(random_node))
  }
  //}
  //END

  var selectnode = nodesIndex(Random.nextInt(nodesIndex.length))
  nodesRefered(selectnode) ! closeProgam()


  // start the routing for random node
  println("\nStarting the Chord Protocol") // testing flow
  println("\nThe Actor node will start passing the messages") // testing flow
  sentcount = 6
  for (jk <- 0 to sentcount) {
    var selectnode = nodesIndex(Random.nextInt(nodesIndex.length))
    var selectdata = dataIndex(Random.nextInt(dataIndex.length))
    println("selectnode:"+selectnode+" selectdata:"+selectdata)
    //nodesRefered(selectnode) ! closeProgam()
    nodesRefered(selectnode) ! searchRoute(selectnode, selectdata, 0)
  }

  var selectnode = nodesIndex(Random.nextInt(nodesIndex.length))
  nodesRefered(selectnode) ! closeProgam()

  def joinNode() {

    nodesRefered(nodesIndex(countg)) ! startActorAgent(countg, nodesIndex(countg), nodesRefered, nodesIndex, dataIndex, pred, succ, no_of_req, m) // starting the actor models
    sleep(2000)
    nodesRefered(nodesIndex(countg)) ! createFingerTable()
    sleep(2000)
    nodesRefered(nodesIndex(countg)) ! createDataTable()
    sleep(2000)

    countg = countg + 1
  }

  def sleep(duration: Long) {
    Thread.sleep(duration)
  }

  // receiver for convergence detection
  def receive = {

    case countHop(hops:Int, success:Int) => {
      counthop = counthop + hops
      if (success == 1) {
        countSucess = countSucess + 1
      }
      else {
        countFailure = countFailure + 1
      }

      Averagehop = counthop /(sentcount+1)
      SuccessRatio = countSucess /(sentcount+1)

      print("\n=================================================================\n")
      println("Number of Hops : "+counthop)
      println("Average Number of Hops : "+Averagehop)
      println("Success : "+ countSucess)
      println("Success Rate : "+ SuccessRatio)
      print("\n=================================================================\n")
    }

    case closeProgam() =>
      println("\n\nExiting the Program")
      context.system.shutdown()

    case _ => //log.info("Invalid Start of Program")
      println("Invalid Condition of Program\n\nExiting")
      context.system.shutdown()

  }

  class Node() extends Actor  {
    //properties of node class
    //var node:Int=0 // node id detection
    var nodesRefered :List[ActorRef] = Nil  // reference list
    var adjacencyList :List[Int] = Nil // neighbour list

    var nodex:Int = 0
    var indexx:Int = 0
    var nodesReferedx:List[ActorRef] = Nil
    var nodeListx:List[Int] = Nil
    var fingerTablex:List[Int] = Nil
    var dataListx:List[Int] = Nil
    var dataIndexx:List[Int] = Nil
    var predx:Int = 0
    var succx:Int = 0
    var dataTablex:List[Int] = Nil
    var requestx:Int = 0

    var mx:Int = 0
    // receiver module
    def receive =  {

      //
      case startActorAgent(index:Int, node:Int, nodesRefered:List[ActorRef], nodeList:List[Int], dataIndex:List[Int], pred:Int, succ:Int, request:Int, m:Int) =>  {
        mx = m
        indexx = index
        nodex = node
        nodesReferedx = nodesRefered
        dataIndexx = dataIndex
        nodeListx = nodeList
        predx = pred
        succx = succ
        requestx = request
        //println(node+" oo "+dataTablex)
        //println("oo "+fingerTablex)
        //println("oo "+requestx)
        //println(predx)
        //println(nodex+"th Agent Created")
        // temporarily used
        //nodesRefered(node) ! closeProgam()
      }

      case createFingerTable() => {
        fingerTablex = Nil

        //println("i "+i)

        // generate fingerlist
        for (j <- 0 to mx - 1) {
          var temp2: Int = nodeListx(indexx)
          //println("temp2 "+temp2)
          var temp4: Int = nodeListx(indexx) + pow(2, j).toInt
          //println("temp4 "+temp4)

          if (temp4 >= pow(2, mx)) {
            temp4 = temp4 % (pow(2, mx).toInt)
          }
          // find the integer in nodesIndex and put in temp2
          var temp5: Int = nodeListx.length
          //println("temp5 "+temp5)
          for (icount <- 0 to temp5 - 1) {
            if (icount == temp5 - 1) {
              if (temp4 >= nodeListx(icount) || temp4 < nodeListx(0)) {
                temp2 = nodeListx(icount)
                //break
              }
            }
            else {
              if (temp4 >= nodeListx(icount) && temp4 < nodeListx(icount + 1)) {
                temp2 = nodeListx(icount)
                //break
              }
            }
          }


          //println("temp2 :"+temp2)
          // add temp2 in fingerList
          fingerTablex ::= temp2
        }

        println("fingerTablex "+fingerTablex)
      }

      case createDataTable() => {
        dataListx = Nil

        // generate datalist

        var start2: Int = nodeListx(indexx)
        var end2: Int = nodeListx(indexx)
        if (indexx == nodeListx.length - 1) {
          end2 = nodeListx(0)
        }
        else {
          end2 = nodeListx(indexx + 1)
        }

        // transfer to dataList
        if (start2 < end2) {
          for (k <- 0 to (no_of_data - 1)) {
            if (dataIndexx(k) >= start2 && dataIndexx(k) < end2) {
              dataListx ::= dataIndexx(k)
            }
          }
        }
        else if (start2 == end2) {
          for (k <- 0 to (no_of_data - 1)) {
            if (dataIndexx(k) == start2 /*&& dataIndex(k) == end2*/ ) {
              dataListx ::= dataIndexx(k)
            }
          }
        }
        else {
          for (k <- 0 to (no_of_data - 1)) {
            if (dataIndexx(k) >= start2 || dataIndexx(k) < end2) {
              dataListx ::= dataIndexx(k)
            }
          }
        }

        //println("start2 "+start2)
        //println("end2 "+end2)

        println("dataListx "+dataListx)

      }

      case searchRoute(nodeID:Int, item:Int, count:Int) => {

        println("Starting Search")
        println("NodeID: "+(nodeID)+" item:"+item+" Request: "+count)


        println(dataListx)
        //println(requestx)
        println(fingerTablex)

        if (count < requestx) { // if the number of request is not exceeding

          //println("hi1")
          // search your own list
          var stamp:Int = -1
          //println(dataTablex)
          for (k1 <- 0 to dataListx.length-1) {
            if (item == dataListx(k1)) {
              stamp = k1
              //break()
            }
          }

          //println("hi2 "+stamp)

          if (stamp != -1) { // if found that is stamp == 1
            // then display results
            println("\n\nFound the file "+item+" at Node "+nodex+"\n\n")
            context.parent ! countHop(count,1)
            //nodesReferedx(nodeID) ! closeProgam()
          }
          else {  // increment the request count and if possible send it to the nearest possible node for search
            // increment the request count and sent to the relevent node for further search

            // send it to the nearest possible node for search
            var temp45:Int = -1

            for (k11 <- 0 to fingerTablex.length - 1){
              if (k11 < fingerTablex.length - 1) {
                if (fingerTablex((fingerTablex.length - 1)-(k11+1)) > fingerTablex((fingerTablex.length - 1)-k11)){
                  if (item >= fingerTablex((fingerTablex.length - 1)-k11) && item < fingerTablex((fingerTablex.length - 1)-(k11+1))) {
                    temp45 = fingerTablex((fingerTablex.length - 1)-k11)
                  }
                }
                else if (fingerTablex((fingerTablex.length - 1)-(k11+1)) == fingerTablex((fingerTablex.length - 1)-k11)) {
                  // do nothing
                }
                else {
                  if (item > fingerTablex((fingerTablex.length - 1)-k11) || item < fingerTablex((fingerTablex.length - 1)-(k11+1))) {
                    temp45 = fingerTablex((fingerTablex.length - 1)-k11)
                  }
                }
              }
              else {
                if (temp45 == -1 || temp45 == nodeID || temp45 == nodex) {
                  temp45 = fingerTablex((fingerTablex.length - 1)-k11)
                }
              }
            }

            // sending
            var temp09:Int = count + 1
            println("temp45 "+temp45+" node "+nodeID+" item "+item+" count "+temp09)
            nodesReferedx(temp45) ! searchRoute(nodeID, item, temp09)
            println("Search continues ... \nSending to node "+temp45)
            //nodesReferedx(temp45) ! closeProgam()
          }
        }
        else { // if the number of request is exceeding
          // send message for exit
          println("\n\nSearch cannot be proceeded as the number of requests exhausted\n\n")
          context.parent ! countHop(10,0)
          //nodesReferedx(nodeID) ! closeProgam()
        }/**/

      }

      case closeProgam() =>
        println("\n\nExiting the Program")
        context.system.shutdown()

      case _ => //log.info("Invalid Start of Program")
        println("Invalid Condition of Program\n\nExiting")
        context.system.shutdown()
    }
  }

}