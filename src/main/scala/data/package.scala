import java.util.Date

import scala.collection._

package object data {

  /* Persons */

  // Raw person data read from CSV
  case class PersonRaw(nom: String, prenom: String, gender: Boolean, birthDate: Option[Date], birthCode: String, deathDate: Option[Date], deathCode: String)

  // Person data read & written from & to the database
  case class PersonData(nom: String, prenom: String, gender: Boolean, birthDate: Option[Date], birthPlaceId: Int, deathDate: Option[Date], deathPlaceId: Int)

  // Processed person data used for queries
  case class PersonProcessed(noms: Seq[Int], prenoms: Seq[Int], gender: Boolean, birthDate: Option[Date], birthPlaceIds: Seq[Int], deathDate: Option[Date], deathPlaceIds: Seq[Int])

  // Person data, in an easily displayable format
  case class PersonDisplay(nom: String, prenom: String, gender: Boolean, birthDate: Option[Date], birthPlace: Option[String], deathDate: Option[Date], deathPlace: Option[String])


  case class PersonQuery(nomsIds: Seq[Int], prenomsIds: Seq[Int], placeIds: Seq[Int]) // TODO add dates


  /* Places */

  // Raw place data read from CSV
  sealed abstract class Place { val name: String }
  case class PlaceOldCommune(inseeCode: String, name: String, communeInseeCode: String) extends Place
  case class PlaceCommune(inseeCode: String, name: String, departementId: String) extends Place
  case class PlaceDepartement(departementId: String, name: String, regionId: String) extends Place
  case class PlaceRegion(regionId: String, name: String) extends Place
  case class PlaceCountry(inseeCode: String, name: String, currentInseeCode: Option[String]) extends Place

  case class PlaceTree(inseeCodeOpt: Option[String], name: String, children: Set[PlaceTree])

  // Place data read & written from & to the database
  case class PlaceData(name: String, parent: Option[Int])

  // Person data, in an easily displayable format
  case class PlaceDisplay(id: Int, fullname: String)

  // TODO

}
