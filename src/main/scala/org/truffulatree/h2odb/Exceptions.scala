// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
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
