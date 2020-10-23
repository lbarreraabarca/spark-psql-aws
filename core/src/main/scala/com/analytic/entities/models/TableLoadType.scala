package com.analytic.entities.models

object TableLoadType extends Enumeration {
  type TableLoadType = Value
  val MOST_RECENT, ALL_DATA, TOTAL, INTERVAL, MOST_RECENT_UNTIL = Value
}
