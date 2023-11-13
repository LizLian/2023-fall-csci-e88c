package org.cscie88c.week10

import cats._
import cats.implicits._

final case class AggregateResult(
  customerId: String,
  averageAmount: Double
)

final case class AverageTansactionAggregate(
  customerId: String,
  totalAmount: Double,
  count: Long
) {
  def averageAmount: Double = totalAmount / count
}


object AverageTansactionAggregate {
  def apply(tx: RawTransaction): AverageTansactionAggregate =
    AverageTansactionAggregate(tx.customer_id, tx.tran_amount, 1L)

  implicit val averageTransactionMonoid: Monoid[AverageTansactionAggregate] =
    new Monoid[AverageTansactionAggregate] {
      override def empty: AverageTansactionAggregate =
        AverageTansactionAggregate("", 0.0, 0L)

      override def combine(x: AverageTansactionAggregate, y: AverageTansactionAggregate): AverageTansactionAggregate =
        AverageTansactionAggregate(
          x.customerId,
          x.totalAmount + y.totalAmount,
          x.count + y.count
        )
    }
}