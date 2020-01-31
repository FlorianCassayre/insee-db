
package object data {

  sealed abstract class Place {
    val name: String
  }

  case class PlaceOldCommune(inseeCode: String, name: String, communeInseeCode: String) extends Place
  case class PlaceCommune(inseeCode: String, name: String, departementId: String) extends Place
  case class PlaceDepartement(departementId: String, name: String, regionId: String) extends Place
  case class PlaceRegion(regionId: String, name: String) extends Place
  case class PlaceCountry(inseeCode: String, name: String) extends Place

  case class PlaceTree(inseeCodeOpt: Option[String], name: String, children: Set[PlaceTree])

}
