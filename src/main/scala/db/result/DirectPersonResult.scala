package db.result

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import data.PersonData
import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils._
import db.util.DateUtils

class DirectPersonResult extends DirectMappingResult[PersonData] {

  private val DateSize = LongSize // TODO reduce date memory footprint (can be represented in <8 bytes)

  override def readResultEntry(context: FileContextIn): PersonData = {
    def readDateOption(context: FileContextIn, offset: Int): Option[LocalDate] = {
      val v = context.readLong(offset)
      if(v != 0) Some(DateUtils.fromMillis(v)) else None
    }
    def readStringMapOption(context: FileContextIn): (Option[Map[String, String]], FileContextIn) = {
      val n = context.readByte(0)
      val reindexed = context.reindex(ByteSize)
      if(n > 0) {
        val (newContext, seq) = (0 until n).foldLeft((reindexed, Seq.empty[(String, String)])) { case ((context1, acc), _) =>
          val (str1, context2) = context1.readString(0)
          val (str2, context3) = context2.readString(0)
          (context3, (str1, str2) +: acc)
        }
        (Some(seq.toMap), newContext)
      } else {
        (None, reindexed)
      }
    }
    var ctx = context
    val noms = {
      val (seq, ctx1) = ctx.readString(0)
      ctx = ctx1
      seq
    }
    val prenoms = {
      val (seq, ctx1) = ctx.readString(0)
      ctx = ctx1
      seq
    }
    val gender = ctx.readByte(0) == 1
    val birthDate = readDateOption(ctx, ByteSize)
    val birthPlace = ctx.readInt(ByteSize + DateSize)
    val deathDate = readDateOption(ctx, ByteSize + DateSize + IntSize)
    val deathPlace = ctx.readInt(ByteSize + DateSize + IntSize + DateSize)
    val actCode = {
      val (seq, ctx1) = ctx.reindex(ByteSize + DateSize + IntSize + DateSize + IntSize).readString(0)
      ctx = ctx1
      seq
    }
    val (wikipedia, _) = readStringMapOption(ctx)

    PersonData(noms, prenoms, gender, birthDate, birthPlace, deathDate, deathPlace, actCode, wikipedia)
  }

  override def writeResultEntry(context: FileContextOut, entry: PersonData): Unit = {
    def writeDateOption(dateOpt: Option[LocalDate]): Unit = {
      context.writeLong(dateOpt.map(DateUtils.toMillis).getOrElse(0))
    }
    def writeStringMapOption(stringMapOpt: Option[Map[String, String]]): Unit = {
      context.writeByte(stringMapOpt.map(_.size).getOrElse(0))
      stringMapOpt.foreach { map =>
        map.toSeq.sorted.foreach { case (key, value) =>
          context.writeString(key)
          context.writeString(value)
        }
      }
    }
    context.writeString(entry.nom)
    context.writeString(entry.prenom)
    context.writeByte(if(entry.gender) 1 else 2)
    writeDateOption(entry.birthDate)
    context.writeInt(entry.birthPlaceId)
    writeDateOption(entry.deathDate)
    context.writeInt(entry.deathPlaceId)
    context.writeString(entry.actCode)
    writeStringMapOption(entry.wikipedia)
  }

}
