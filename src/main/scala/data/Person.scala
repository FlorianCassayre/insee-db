package data

import java.util.Date

case class Person(nom: String, prenom: String, gender: Boolean, birthDate: Option[Date], birthCode: String, deathDate: Option[Date], deathCode: String)