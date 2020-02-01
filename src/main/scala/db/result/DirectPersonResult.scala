package db.result

import java.util.Date

import data.PersonData
import db.file.FileContext
import db.util.DatabaseUtils._

class DirectPersonResult extends DirectMappingResult[PersonData] {

  private val DateSize = LongSize // TODO reduce date memory footprint (can be represented in <8 bytes)

  override def readResultEntry(context: FileContext): PersonData = {
    def readDateOption(context: FileContext, offset: Int): Option[Date] = {
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

  override def writeResultEntry(context: FileContext, entry: PersonData): FileContext = {
    def writeDateOption(context: FileContext, offset: Int, dateOpt: Option[Date]): Unit = {
      context.writeLong(offset, dateOpt.map(_.getTime).getOrElse(0))
    }
    def writeIntOption(context: FileContext, offset: Int, option: Option[Int]): Unit = context.writeInt(offset, option.getOrElse(-1))
    var ctx = context
    ctx = ctx.writeString(0, entry.nom)
    ctx = ctx.writeString(0, entry.prenom)
    ctx.writeByte(0, if(entry.gender) 1 else 2)
    writeDateOption(ctx, ByteSize, entry.birthDate)
    ctx.writeInt(ByteSize + DateSize, entry.birthPlaceId)
    writeDateOption(ctx, ByteSize + DateSize + IntSize, entry.deathDate)
    ctx.writeInt(ByteSize + DateSize + IntSize + DateSize, entry.deathPlaceId)

    ctx.reindex(ByteSize + 2 * (DateSize + IntSize))
  }

}
