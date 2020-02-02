package db.result

class ReferenceResult[Q, P] extends LimitedReferenceResult[Q, P] {

  override val MaxResults: Int = Int.MaxValue

}
