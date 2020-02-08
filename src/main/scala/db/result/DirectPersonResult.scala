package db.result

import java.util.Date

import data.PersonData
import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils._

class DirectPersonResult extends DirectMappingResult[PersonData] {

  private val DateSize = LongSize // TODO reduce date memory footprint (can be represented in <8 bytes)

  override def readResultEntry(context: FileContextIn): PersonData = {
    def readDateOption(context: FileContextIn, offset: Int): Option[Date] = {
      val v = context.readLong(offset)
      if(v != 0) Some(new Date(v)) else None
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

    PersonData(noms, prenoms, gender, birthDate, birthPlace, deathDate, deathPlace)
  }

  override def writeResultEntry(context: FileContextOut, entry: PersonData): Unit = {
    def writeDateOption(dateOpt: Option[Date]): Unit = {
      context.writeLong(dateOpt.map(_.getTime).getOrElse(0))
    }
    def writeIntOption(context: FileContextOut, offset: Int, option: Option[Int]): Unit = context.writeInt(option.getOrElse(-1))
    context.writeString(entry.nom)
    context.writeString(entry.prenom)
    context.writeByte(if(entry.gender) 1 else 2)
    writeDateOption(entry.birthDate)
    context.writeInt(entry.birthPlaceId)
    writeDateOption(entry.deathDate)
    context.writeInt(entry.deathPlaceId)
  }

}
