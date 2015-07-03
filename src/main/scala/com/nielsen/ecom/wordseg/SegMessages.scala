package com.nielsen.ecom.wordseg

case class SegJob(text: String)
case class SegResult(meanWordLength: String)
case class JobFailed(reason: String, job: SegJob)
case object BackendRegistration