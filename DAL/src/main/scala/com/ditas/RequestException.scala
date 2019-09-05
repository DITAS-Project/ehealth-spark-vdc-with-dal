package com.ditas

final case class RequestException(private val message: String = "",
                                  private val cause: Throwable = None.orNull) extends Exception(message, cause)

