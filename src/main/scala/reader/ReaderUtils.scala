package reader

object ReaderUtils {

  // https://stackoverflow.com/a/48494552/4413709
  def capitalizeFirstPerWord(str: String): String = raw"\b((?<!\b')\w+)".r.replaceAllIn(str.toLowerCase, _.group(1).capitalize)

}
