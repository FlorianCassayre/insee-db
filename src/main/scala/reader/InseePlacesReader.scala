package reader

import java.io.File
import data._
import db.util.StringUtils
import reader.ReaderUtils._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * https://www.insee.fr/fr/information/3720946
 */
object InseePlacesReader {

  def readCommunes(file: File): (Seq[PlaceOldCommune], Seq[PlaceCommune]) = {
    val (oldCommunes, currentCommunes) = csvReader(file).map { r =>
      val (inseeCode, name, departementId, parent) = (r.get(1), r.get(8), r.get(3), r.get(10))
      val isOld = departementId.isEmpty
      if(isOld) (Some(PlaceOldCommune(inseeCode, name, parent)), None)
      else (None, Some(PlaceCommune(inseeCode, name, departementId)))
    }.toVector.unzip
    (oldCommunes.flatten, currentCommunes.flatten)
  }

  def readDepartements(file: File): Seq[PlaceDepartement] =
    csvReader(file).map(r =>
      PlaceDepartement(r.get(0), r.get(6), r.get(1))
    ).toVector

  def readRegions(file: File): Seq[PlaceRegion] =
    csvReader(file).map(r =>
      PlaceRegion(r.get(0), r.get(5))
    ).toVector

  def readCountries(file: File): Seq[PlaceCountry] =
    csvReader(file).map { r =>
      val parent = r.get(3)
      PlaceCountry(r.get(0), StringUtils.capitalizeFirstPerWord(r.get(5)), if(parent.nonEmpty) Some(parent) else None)
    }.toVector

  def readCommunesEvents(file: File): Map[String, Seq[(String, String)]] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    // Map[oldCode, Seq[(newName, newCode)]]
    val oldCodeToOldNameNewCodeDate: Map[String, Seq[(String, String, LocalDate)]] = csvReader(file).flatMap { r =>
      val (date, oldCode, oldName, newCode) = (LocalDate.parse(r.get(1), formatter), r.get(3), r.get(7), r.get(9))
      if(newCode.length >= 5 && oldCode != newCode)
        Seq(oldCode -> (oldName, newCode, date))
      else
        Seq.empty
    }.toSeq.groupBy { case (oldCode, _) => oldCode }
      .view.mapValues(_.map { case (_, values) => values }.sortBy { case (_, _, date) => date }.reverse).toMap
    def uppest(id: String, visited: Set[String] = Set.empty): String = {
      if(visited.contains(id)) { // Cycle?
        val latestChange = visited.toSeq.maxBy { id =>
          val (_, _, date) = oldCodeToOldNameNewCodeDate(id).head
          date
        }
        val (_, latestId, _) = oldCodeToOldNameNewCodeDate(latestChange).head
        latestId
      } else {
        if(oldCodeToOldNameNewCodeDate.contains(id)) {
          val (_, nextId, _) = oldCodeToOldNameNewCodeDate(id).head
          uppest(nextId, visited + id)
        } else {
          id
        }
      }
    }
    val allCodes = oldCodeToOldNameNewCodeDate.keySet
    allCodes.map { oldId =>
      val parentId = uppest(oldId) // Representative
      val (oldName, _, _) = oldCodeToOldNameNewCodeDate(oldId).head
      parentId -> (oldId, oldName)
    }.groupBy { case (parentId, _) => parentId }.view.mapValues(_.map { case (_, values) => values }.toSeq).toMap
  }


  def readPlaces(fileCommunes: File, fileDepartements: File, fileRegions: File, fileCountries: File, fileCommunesEvents: File): (PlaceTree, Map[String, String]) = {
    val (oldCommunes, communes) = readCommunes(fileCommunes)
    val departements = readDepartements(fileDepartements)
    val regions = readRegions(fileRegions)
    val countries = readCountries(fileCountries)
    val communesEvents = readCommunesEvents(fileCommunesEvents)

    val France = "France"

    val tree = PlaceTree(None, "", countries.filter(_.currentInseeCode.isEmpty).map(country =>
      PlaceTree(Some(country.inseeCode), country.name, if(country.name == France)
        regions.map(r =>
          PlaceTree(None, r.name,
            departements.filter(_.regionId == r.regionId).map(d =>
              PlaceTree(None, d.name,
                communes.filter(_.departementId == d.departementId).map(commune =>
                  PlaceTree(Some(commune.inseeCode), commune.name, {
                    val oldFound = oldCommunes.filter(_.communeInseeCode == commune.inseeCode)
                    val oldSet = oldFound.map(_.inseeCode).toSet
                    val old1 = oldFound.map(old =>
                      PlaceTree(Some(old.inseeCode), old.name, Set.empty)
                    ).toSet
                    val old2 = communesEvents.getOrElse(commune.inseeCode, Seq.empty).filter(t => !oldSet.contains(t._1)).map(old =>
                      PlaceTree(Some(old._1), old._2, Set.empty)
                    )
                    old1 ++ old2 // Dirty but mostly works
                  }
                  )
                ).toSet
              )
            ).toSet
          )
        ).toSet
      else Set.empty)
    ).toSet)

    val countryTranslation = countries.flatMap(old => old.currentInseeCode.map(current => old.inseeCode -> current)).toMap

    (tree, countryTranslation)
  }

  def readPlaces(directory: File): (PlaceTree, Map[String, String]) = {
    require(directory.isDirectory)

    def sub(filename: String): File = new File(directory.getAbsolutePath + "/" + filename)

    // Note: this format is not standard (but convenient)
    readPlaces(sub("communes.csv"), sub("departements.csv"), sub("regions.csv"), sub("pays.csv"), sub("mouvements.csv"))
  }

}
