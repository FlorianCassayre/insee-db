package static

object Geography {

  case class StaticPlace(code: String, name: String, children: Seq[StaticPlace])

  val StaticPlaces: Seq[StaticPlace] = Seq(
    // Countries
    p(c("FR"), "France", Seq(
      // French regions
      p(r("1"), "Guadeloupe", Seq(
        p(d("971"), "Guadeloupe")
      )),
      p(r("2"), "Martinique", Seq(
        p(d("972"), "Martinique")
      )),
      p(r("3"), "Guyane", Seq(
        p(d("973"), "Guyane")
      )),
      p(r("4"), "La Réunion", Seq(
        p(d("974"), "La Réunion")
      )),
      p(r("6"), "Mayotte", Seq(
        p(d("976"), "Mayotte"),
      )),
      p(r("11"), "Île-de-France", Seq(
        p(d("75"), "Paris"),
        p(d("77"), "Seine-et-Marne"),
        p(d("78"), "Yvelines"),
        p(d("91"), "Essonne"),
        p(d("92"), "Hauts-de-Seine"),
        p(d("93"), "Seine-Saint-Denis"),
        p(d("94"), "Val-de-Marne"),
        p(d("95"), "Val-d'Oise"),
      )),
      p(r("24"), "Centre-Val de Loire", Seq(
        p(d("18"), "Cher"),
        p(d("28"), "Eure-et-Loir"),
        p(d("36"), "Indre"),
        p(d("37"), "Indre-et-Loire"),
        p(d("41"), "Loir-et-Cher"),
        p(d("45"), "Loiret"),
      )),
      p(r("27"), "Bourgogne-Franche-Comté", Seq(
        p(d("21"), "Côte-d'Or"),
        p(d("25"), "Doubs"),
        p(d("39"), "Jura"),
        p(d("58"), "Nièvre"),
        p(d("70"), "Haute-Saône"),
        p(d("71"), "Saône-et-Loire"),
        p(d("89"), "Yonne"),
        p(d("90"), "Territoire de Belfort"),
      )),
      p(r("28"), "Normandie", Seq(
        p(d("14"), "Calvados"),
        p(d("27"), "Eure"),
        p(d("50"), "Manche"),
        p(d("61"), "Orne"),
        p(d("76"), "Seine-Maritime"),
      )),
      p(r("32"), "Hauts-de-France", Seq(
        p(d("02"), "Aisne"),
        p(d("59"), "Nord"),
        p(d("60"), "Oise"),
        p(d("62"), "Pas-de-Calais"),
        p(d("80"), "Somme"),
      )),
      p(r("44"), "Grand Est", Seq(
        p(d("08"), "Ardennes"),
        p(d("10"), "Aube"),
        p(d("51"), "Marne"),
        p(d("52"), "Haute-Marne"),
        p(d("54"), "Meurthe-et-Moselle"),
        p(d("55"), "Meuse"),
        p(d("57"), "Moselle"),
        p(d("67"), "Bas-Rhin"),
        p(d("68"), "Haut-Rhin"),
        p(d("88"), "Vosges"),
      )),
      p(r("52"), "Pays de la Loire", Seq(
        p(d("44"), "Loire-Atlantique"),
        p(d("49"), "Maine-et-Loire"),
        p(d("53"), "Mayenne"),
        p(d("72"), "Sarthe"),
        p(d("85"), "Vendée"),
      )),
      p(r("53"), "Bretagne", Seq(
        p(d("22"), "Côtes-d'Armor"),
        p(d("29"), "Finistère"),
        p(d("35"), "Ille-et-Vilaine"),
        p(d("56"), "Morbihan"),
      )),
      p(r("75"), "Nouvelle-Aquitaine", Seq(
        p(d("16"), "Charente"),
        p(d("17"), "Charente-Maritime"),
        p(d("19"), "Corrèze"),
        p(d("23"), "Creuse"),
        p(d("24"), "Dordogne"),
        p(d("33"), "Gironde"),
        p(d("40"), "Landes"),
        p(d("47"), "Lot-et-Garonne"),
        p(d("64"), "Pyrénées-Atlantiques"),
        p(d("79"), "Deux-Sèvres"),
        p(d("86"), "Vienne"),
        p(d("87"), "Haute-Vienne"),
      )),
      p(r("76"), "Occitanie", Seq(
        p(d("09"), "Ariège"),
        p(d("11"), "Aude"),
        p(d("12"), "Aveyron"),
        p(d("30"), "Gard"),
        p(d("31"), "Haute-Garonne"),
        p(d("32"), "Gers"),
        p(d("34"), "Hérault"),
        p(d("46"), "Lot"),
        p(d("48"), "Lozère"),
        p(d("65"), "Hautes-Pyrénées"),
        p(d("66"), "Pyrénées-Orientales"),
        p(d("81"), "Tarn"),
        p(d("82"), "Tarn-et-Garonne"),
      )),
      p(r("84"), "Auvergne-Rhône-Alpes", Seq(
        p(d("01"), "Ain"),
        p(d("03"), "Allier"),
        p(d("07"), "Ardèche"),
        p(d("15"), "Cantal"),
        p(d("26"), "Drôme"),
        p(d("38"), "Isère"),
        p(d("42"), "Loire"),
        p(d("43"), "Haute-Loire"),
        p(d("63"), "Puy-de-Dôme"),
        p(d("69"), "Rhône"),
        p(d("73"), "Savoie"),
        p(d("74"), "Haute-Savoie"),
      )),
      p(r("93"), "Provence-Alpes-Côte d'Azur", Seq(
        p(d("04"), "Alpes-de-Haute-Provence"),
        p(d("05"), "Hautes-Alpes"),
        p(d("06"), "Alpes-Maritimes"),
        p(d("13"), "Bouches-du-Rhône"),
        p(d("83"), "Var"),
        p(d("84"), "Vaucluse"),
      )),
      p(r("94"), "Corse", Seq(
        p(d("2A"), "Corse-du-Sud"),
        p(d("2B"), "Haute-Corse"),
      )),
    )),
  )

  private def c(id: String): String = s"C-$id"
  private def r(id: String): String = s"R-$id"
  private def d(id: String): String = s"D-$id"

  private def p(code: String, name: String, children: Seq[StaticPlace] = Seq.empty): StaticPlace =
    StaticPlace(code, name, children)

}
