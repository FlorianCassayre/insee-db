import java.time.LocalDate

import scala.collection._

package object data {

  /* Persons */

  // Raw person data read from CSV
  case class PersonRaw(nom: String,
                       prenom: String,
                       gender: Boolean,
                       birthDate: Option[LocalDate],
                       birthCode: String,
                       deathDate: Option[LocalDate],
                       deathCode: String,
                       actCode: String)

  // Person data as read/written from/to the database
  case class PersonData(nom: String,
                        prenom: String,
                        gender: Boolean,
                        birthDate: Option[LocalDate],
                        birthPlaceId: Int,
                        deathDate: Option[LocalDate],
                        deathPlaceId: Int,
                        actCode: String,
                        wikipedia: Option[collection.immutable.Map[String, String]]
                       )

  // Processed person data to be written to the index
  case class PersonProcessed(noms: Seq[Int],
                             prenoms: Seq[Int],
                             gender: Boolean,
                             birthDate: Option[LocalDate],
                             birthPlaceIds: Seq[Int],
                             deathDate: Option[LocalDate],
                             deathPlaceIds: Seq[Int])

  // Query parameters for simple search
  abstract class AbstractNamePlaceQuery {
    val nomsIds: Seq[Int]
    val prenomsIds: Seq[Int]
    val placeIds: Seq[Int]
  }

  abstract class AbstractNamePlaceDateAttributeQuery extends AbstractNamePlaceQuery {
    val filterByBirth: Boolean
  }

  // Query parameters performed by the end user after processing
  case class PersonQuery(nomsIds: Seq[Int],
                         prenomsIds: Seq[Int],
                         placeIds: Seq[Int],
                         filterByBirth: Boolean,
                         ascending: Boolean,
                         yearMin: Option[Int],
                         yearMax: Option[Int]) extends AbstractNamePlaceDateAttributeQuery

  // Query parameters for geographical statistics queries
  case class PlaceStatisticsQuery(nomsIds: Seq[Int],
                                  prenomsIds: Seq[Int],
                                  placeIds: Seq[Int],
                                  nestingDepth: Int) extends AbstractNamePlaceQuery

  // Query parameters for temporal statistics queries
  case class TimeStatisticsQuery(nomsIds: Seq[Int],
                                 prenomsIds: Seq[Int],
                                 placeIds: Seq[Int],
                                 filterByBirth: Boolean) extends AbstractNamePlaceDateAttributeQuery

  // Person data, in an easily displayable format
  case class PersonDisplay(nom: String,
                           prenom: String,
                           gender: Boolean,
                           birthDate: Option[LocalDate],
                           birthPlace: Option[String],
                           deathDate: Option[LocalDate],
                           deathPlace: Option[String],
                           actCode: String,
                           wikipedia: Option[collection.immutable.Map[String, String]])


  /* Places */

  // Raw place data read from CSV
  sealed abstract class Place {
    val name: String
  }

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

  /* Other */

  case class PersonBlackListed(deathDate: LocalDate, deathPlaceCode: String, actCode: String)

  case class WikiDataQueryResult(person: String,
                                 personName: Option[String],
                                 personNameBirth: Option[String],
                                 personAltLabel: Option[String],
                                 personBirthDate: Option[LocalDate],
                                 personDeathDate: Option[LocalDate],
                                 personArticleFr: Option[String],
                                 personArticleEn: Option[String])

}
