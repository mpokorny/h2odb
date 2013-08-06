// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

object Tables {
  /** CSV file "Param" column values
    */
  private object Params {
    val alkalinity      = "Alkalinity as CaCO3"
    val aluminum        = "Aluminum"
    val anions          = "Anions total"
    val antimony        = "Antimony 121"
    val arsenic         = "Arsenic"
    val barium          = "Barium"
    val beryllium       = "Beryllium"
    val bicarbonate     = "Bicarbonate (HCO3)"
    val boron           = "Boron 11"
    val bromide         = "Bromide"
    val cadmium         = "Cadmium 111"
    val calcium         = "Calcium"
    val carbonate       = "Carbonate (CO3)"
    val cations         = "Cations total"
    val chloride        = "Chloride"
    val chromium        = "Chromium"
    val cobalt          = "Cobalt"
    val copper          = "Copper 65"
    val fluoride        = "Fluoride"
    val hardness        = "Hardness"
    val iron            = "Iron"
    val lead            = "Lead"
    val lithium         = "Lithium"
    val magnesium       = "Magnesium"
    val manganese       = "Manganese"
    val molybdenum      = "Molybdenum 95"
    val nickel          = "Nickel"
    val nitrate         = "Nitrate"
    val nitrite         = "Nitrite"
    val phosphate       = "Ortho Phosphate"
    val percentDiff     = "Percent difference"
    val potassium       = "Potassium"
    val selenium        = "Selenium"
    val siliconDioxide  = "SiO2"
    val silicon         = "Silicon"
    val silver          = "Silver 107"
    val sodium          = "Sodium"
    val conductance     = "Specific Conductance"
    val strontium       = "Strontium"
    val sulfate         = "Sulfate"
    val tds             = "TDS calc"
    val thallium        = "Thallium"
    val thorium         = "Thorium"
    val tin             = "Tin"
    val titanium        = "Titanium"
    val uranium         = "Uranium"
    val vanadium        = "Vanadium"
    val zinc            = "Zinc 66"
    val pH              = "pH"
  }

  import Params._

  /** Map from CSV file "Param" values to DB table analyte values
    */
  val analytes = Map(
    alkalinity     -> "ALK",
    aluminum       -> "Al",
    anions         -> "TAn",
    antimony       -> "Sb",
    arsenic        -> "As",
    barium         -> "Ba",
    beryllium      -> "Be",
    bicarbonate    -> "HCO3",
    boron          -> "B",
    bromide        -> "Br",
    cadmium        -> "Cd",
    calcium        -> "Ca",
    carbonate      -> "CO3",
    cations        -> "TCat",
    chloride       -> "Cl",
    chromium       -> "Cr",
    cobalt         -> "Co",
    copper         -> "Cu",
    fluoride       -> "F",
    hardness       -> "HRD",
    iron           -> "Fe",
    lead           -> "Pb",
    lithium        -> "Li",
    magnesium      -> "Mg",
    manganese      -> "Mn",
    molybdenum     -> "Mo",
    nickel         -> "Ni",
    nitrate        -> "NO3",
    nitrite        -> "NO2",
    phosphate      -> "PO4",
    percentDiff    -> "IONBAL",
    potassium      -> "K",
    selenium       -> "Se",
    siliconDioxide -> "SiO2",
    silicon        -> "Si",
    silver         -> "Ag",
    sodium         -> "Na",
    conductance    -> "CONDLAB",
    strontium      -> "Sr",
    sulfate        -> "SO4",
    tds            -> "TDS",
    thallium       -> "Tl",
    thorium        -> "Th",
    tin            -> "Sn",
    titanium       -> "Ti",
    uranium        -> "U",
    vanadium       -> "V",
    zinc           -> "Zn",
    pH             -> "pHL")

  /** Map from CSV "Param" values to DB "AnalysisMethod" values
    */
  val method = Map(
    alkalinity  -> "as CaCO3",
    bicarbonate -> "Alkalinity as HCO3",
    tds         -> "Calculation",
    hardness    -> "as CaCO3")

  /** Map from CSV "Results_Units" values to DB "Units" values, by "Param"
    */
  val units = Map(
    anions      -> "epm",
    cations     -> "epm",
    hardness    -> "mg/L",
    percentDiff -> "%Diff",
    pH          -> "pH",
    conductance -> "µS/cm")

  /** Name of "major chemistry" DB table */
  val major = "MajorChemistry"

  /** Name of "minor chemistry" DB table */
  val minor = "MinorandTraceChemistry"

  /** Map from CSV "Param" value to associated DB table
    */
  val chemistryTable = Map(
    alkalinity     -> major,
    aluminum       -> minor,
    anions         -> major,
    antimony       -> minor,
    arsenic        -> minor,
    barium         -> minor,
    beryllium      -> minor,
    bicarbonate    -> major,
    boron          -> minor,
    bromide        -> minor,
    cadmium        -> minor,
    calcium        -> major,
    carbonate      -> major,
    cations        -> major,
    chloride       -> major,
    chromium       -> minor,
    cobalt         -> minor,
    copper         -> minor,
    fluoride       -> minor,
    hardness       -> major,
    iron           -> minor,
    lead           -> minor,
    lithium        -> minor,
    magnesium      -> major,
    manganese      -> minor,
    molybdenum     -> minor,
    nickel         -> minor,
    nitrate        -> minor,
    nitrite        -> minor,
    phosphate      -> minor,
    percentDiff    -> major,
    potassium      -> major,
    selenium       -> minor,
    siliconDioxide -> minor,
    silicon        -> minor,
    silver         -> minor,
    sodium         -> major,
    conductance    -> major,
    strontium      -> minor,
    sulfate        -> major,
    tds            -> major,
    thallium       -> minor,
    thorium        -> minor,
    tin            -> minor,
    titanium       -> minor,
    uranium        -> minor,
    vanadium       -> minor,
    zinc           -> minor,
    pH             -> major)

  /** Map from CSV "Param" values to list of "Test" values, in priority order
    */
  val testPriority = Map(
    // The order of list elements is from most preferred to least preferred
    strontium   -> List("Trace Metals by ICPMS", "Cations by ICPOES"),
    bromide     -> List("Low Bromide", "Anions by IC"))

  /** Map from DB "Analyte" values to range of acceptable values derived from
    * drinking water quality standards.
    */
  val standards = Map(
    analytes(aluminum)       -> ((0.0f, 0.05f)),
    analytes(antimony)       -> ((0.0f, 0.006f)),
    analytes(arsenic)        -> ((0.0f, 0.01f)),
    analytes(barium)         -> ((0.0f, 2.0f)),
    analytes(beryllium)      -> ((0.0f, 0.004f)),
    analytes(cadmium)        -> ((0.0f, 0.005f)),
    analytes(chloride)       -> ((0.0f, 250.0f)),
    analytes(chromium)       -> ((0.0f, 0.1f)),
    analytes(copper)         -> ((0.0f, 1.0f)),
    analytes(fluoride)       -> ((0.0f, 2.0f)),
    analytes(hardness)       -> ((0.0f, 300.0f)),
    analytes(iron)           -> ((0.0f, 0.3f)),
    analytes(lead)           -> ((0.0f, 0.015f)),
    analytes(manganese)      -> ((0.0f, 0.05f)),
    analytes(nickel)         -> ((0.0f, 0.1f)),
    analytes(nitrate)        -> ((0.0f, 45.0f)),
    analytes(nitrite)        -> ((0.0f, 3.3f)),
    analytes(selenium)       -> ((0.0f, 0.05f)),
    analytes(silver)         -> ((0.0f, 0.1f)),
    analytes(sodium)         -> ((0.0f, 20.0f)),
    analytes(sulfate)        -> ((0.0f, 250.0f)),
    analytes(tds)            -> ((0.0f, 500.0f)),
    analytes(thallium)       -> ((0.0f, 0.002f)),
    analytes(uranium)        -> ((0.0f, 0.03f)),
    analytes(zinc)           -> ((0.0f, 5.0f)),
    analytes(pH)             -> ((6.5f, 8.5f)))
}
