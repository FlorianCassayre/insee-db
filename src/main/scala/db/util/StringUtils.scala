package db.util

object StringUtils {

  def unaccent(string: String): String = {
    import java.text.Normalizer
    Normalizer.normalize(string, Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
  }

  def cleanSplit(str: String, preserveDigits: Boolean): IndexedSeq[String] = str.trim.split(if(preserveDigits) "[^a-z0-9]+" else "[^a-z]+").toVector.filter(_.nonEmpty)

  def normalizeString(str: String): String = unaccent(str.trim.toLowerCase) // TODO

  def normalizeSentence(str: String, preserveDigits: Boolean = false): String = cleanSplitAndNormalize(str, preserveDigits).mkString(" ")

  def cleanSplitAndNormalize(str: String, preserveDigits: Boolean = false): IndexedSeq[String] = cleanSplit(normalizeString(str), preserveDigits)

  // https://stackoverflow.com/a/48494552/4413709
  // https://stackoverflow.com/a/26900132/4413709
  def capitalizeFirstPerWord(str: String): String = raw"\b((?<!\b')[A-Za-zÀ-ÖØ-öø-ÿ]+)".r.replaceAllIn(str.toLowerCase, _.group(1).capitalize)

}
