package util

import java.text.SimpleDateFormat
import java.util.Date

import data.PersonDisplay

import scala.collection.Seq

object CliUtils {

  val PersonFieldsHeader: Seq[String] = Seq("Nom", "Prénom", "Sexe", "Date naissance", "Lieu naissance", "Date décès", "Lieu décès")

  private val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")

  def personToFields(person: PersonDisplay): Seq[String] = {
    def dateToString(date: Option[Date]): String = date.map(simpleDateFormat.format).getOrElse("")
    def placeToString(place: Option[String]): String = place.getOrElse("")
    Seq(person.nom, person.prenom, if(person.gender) "M" else "F", dateToString(person.birthDate), placeToString(person.birthPlace), dateToString(person.deathDate), placeToString(person.deathPlace))
  }

  def asciiTable(header: Seq[String], values: Seq[Seq[String]]): String = {
    val n = header.size

    require(values.forall(_.size == n))

    if(n > 0) {
      val Space = ' '
      val Edge = '+'
      val Pipe = '|'
      val Dash = '-'

      val all = header +: values
      val lengths = (0 until n).map(i => all.map(_ (i).length).max)

      def mkAround(seq: Seq[String], char: Char): String = char + seq.mkString(char.toString) + char

      val borderHorizontal = mkAround(lengths.map(Dash.toString * _), Edge)
      val lines = all.map(r => mkAround(r.zip(lengths).map { case (s, i) => s + (Space.toString * (i - s.length)) }, Pipe))

      (Seq(borderHorizontal, lines.head, borderHorizontal) ++ lines.tail :+ borderHorizontal).mkString("\n")
    } else {
      "(empty table)"
    }
  }

}
