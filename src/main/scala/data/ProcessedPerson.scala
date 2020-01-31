package data

import java.util.Date

case class ProcessedPerson(noms: Seq[Int], prenoms: Seq[Int], gender: Boolean, birthDate: Option[Date], birthCode: Seq[Int], deathDate: Option[Date], deathCode: Seq[Int])