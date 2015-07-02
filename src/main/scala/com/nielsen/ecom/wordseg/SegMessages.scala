package com.nielsen.ecom.wordseg

case class StatsJob(text: String)
case class StatsResult(meanWordLength: String)
case class JobFailed(reason: String)
