package db

import java.io.File

import data.{PersonDisplay, PersonQuery, PlaceDisplay}
import db.util.StringUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

import scala.collection.Seq

class ParallelInseeDatabase(root: File) {

  private val InstancesCount = 50

  private val instances = IndexedSeq.fill(InstancesCount)(new InseeDatabase(root))

  // Greedy instance selection
  var instanceIndex = 0
  private def getInstance(): InseeDatabase = this.synchronized {
    val instance = instances(instanceIndex)
    instanceIndex = (instanceIndex + 1) % instances.size
    instance
  }

  def queryPersons(offset: Int, limit: Int,
                   surname: Option[String] = None,
                   name: Option[String] = None,
                   placeId: Option[Int] = None,
                   filterByBirth: Boolean = true,
                   after: Option[Int] = None, before: Option[Int] = None,
                   ascending: Boolean = true
                  ): ResultSet[PersonDisplay] = {
    require(limit >= 0 && offset >= 0)

    def processName(name: Option[String], translation: (InseeDatabase, String) => Option[Int]): Future[Option[Seq[Int]]] = Future {
      val result = name.map(StringUtils.cleanSplitAndNormalize).getOrElse(Seq.empty).map(s => translation(getInstance(), s))
      if(result.exists(_.isEmpty))
        None
      else
        Some(result.flatten)
    }

    val (surnamesOptFuture, namesOptFuture) = (processName(surname, (db, s) => db.surnameToId(s)), processName(name, (db, s) => db.nameToId(s)))
    val placeOptFuture = Future { // TODO parallelize this even more
      placeId.map(getInstance().idToAbsolutePlace) match {
        case Some(None) => None
        case other => Some(other.flatten.getOrElse(Seq(0))) // `0` is the root place
      }
    }

    val translatedFuture = for {
      surnamesOpt <- surnamesOptFuture
      namesOpt <- namesOptFuture
      placeOpt <- placeOptFuture
    } yield (surnamesOpt, namesOpt, placeOpt)

    Await.result(translatedFuture, Duration.Inf) match {
      case (Some(surnames), Some(names), Some(place)) =>

        val parameters = PersonQuery(surnames, names, place, filterByBirth = filterByBirth, ascending = ascending, after.map(_ - instances.head.BaseYear), before.map(_ - instances.head.BaseYear))

        val resultIds = getInstance().queryPersonsId(offset, limit, parameters)

        val resultFuture = Future.sequence(resultIds.entries.map(id => Future(getInstance().idToPersonDisplay(id).get))) // TODO parallelize idToPersonDisplay

        val persons = Await.result(resultFuture, Duration.Inf)

        resultIds.copy(entries = persons)
      case _ => // A field contains a non existent key
        new ResultSet[PersonDisplay](Seq.empty, 0)
    }
  }

  def queryPlacesByPrefix(limit: Int, prefix: String): Seq[PlaceDisplay] = {
    getInstance().queryPlacesByPrefix(limit, prefix)
  }

}
