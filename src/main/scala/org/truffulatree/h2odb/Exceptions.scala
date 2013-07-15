package org.truffulatree.h2odb

trait H2ODbException

class InvalidInputHeader(message: String)
    extends Exception(message)
    with H2ODbException

class MissingSamplePointID(message: String)
    extends Exception(message)
    with H2ODbException

class MissingParamConversion(message: String)
    extends Exception(message)
    with H2ODbException
