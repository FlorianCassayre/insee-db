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
    val actCode = ctx.reindex(ByteSize + DateSize + IntSize + DateSize + IntSize).readString(0)._1

    PersonData(noms, prenoms, gender, birthDate, birthPlace, deathDate, deathPlace, actCode)
  }

  private val dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy");

  override def writeResultEntry(context: FileContextOut, entry: PersonData): Unit = {
    def writeDateOption(dateOpt: Option[LocalDate]): Unit = {
      context.writeLong(dateOpt.map(DateUtils.toMillis).getOrElse(0))
    }
    def writeIntOption(context: FileContextOut, offset: Int, option: Option[Int]): Unit = context.writeInt(option.getOrElse(-1))
    context.writeString(entry.nom)
    context.writeString(entry.prenom)
    context.writeByte(if(entry.gender) 1 else 2)
    writeDateOption(entry.birthDate)
    context.writeInt(entry.birthPlaceId)
    writeDateOption(entry.deathDate)
    context.writeInt(entry.deathPlaceId)
    context.writeString(entry.actCode)
  }

}
