package db

case class ResultSet[T](entries: scala.collection.Seq[T], total: Int)
