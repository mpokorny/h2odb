// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

trait Tables {

  import AnalysisReport.Params

  /** Map from XLS file "Param" values to DB table analyte values
    */
  val analytes = Map(
      Params.alkalinity     -> "ALK",
      Params.aluminum       -> "Al",
      Params.anions         -> "TAn",
      Params.antimony       -> "Sb",
      Params.arsenic        -> "As",
      Params.barium         -> "Ba",
      Params.beryllium      -> "Be",
      Params.bicarbonate    -> "HCO3",
      Params.boron          -> "B",
      Params.bromide        -> "Br",
      Params.cadmium        -> "Cd",
      Params.calcium        -> "Ca",
      Params.carbonate      -> "CO3",
      Params.cations        -> "TCat",
      Params.chloride       -> "Cl",
      Params.chromium       -> "Cr",
      Params.cobalt         -> "Co",
      Params.copper         -> "Cu",
      Params.fluoride       -> "F",
      Params.hardness       -> "HRD",
      Params.iron           -> "Fe",
      Params.lead           -> "Pb",
      Params.lithium        -> "Li",
      Params.magnesium      -> "Mg",
      Params.manganese      -> "Mn",
      Params.mercury        -> "Hg",
      Params.molybdenum     -> "Mo",
      Params.nickel         -> "Ni",
      Params.nitrate        -> "NO3",
      Params.nitrite        -> "NO2",
      Params.phosphate      -> "PO4",
      Params.percentDiff    -> "IONBAL",
      Params.potassium      -> "K",
      Params.selenium       -> "Se",
      Params.siliconDioxide -> "SiO2",
      Params.silicon        -> "Si",
      Params.silver         -> "Ag",
      Params.sodium         -> "Na",
      Params.conductance    -> "CONDLAB",
      Params.strontium      -> "Sr",
      Params.sulfate        -> "SO4",
      Params.tds            -> "TDS",
      Params.thallium       -> "Tl",
      Params.thorium        -> "Th",
      Params.tin            -> "Sn",
      Params.titanium       -> "Ti",
      Params.uranium        -> "U",
      Params.vanadium       -> "V",
      Params.zinc           -> "Zn",
      Params.pH             -> "pHL")


  /** DB tables info
    */
  trait DbInfo {

    val sampleValue     = "SampleValue"
    val symbol          = "Symbol"
    val samplePointId   = "SamplePointID"
    val samplePointGUID = "SamplePtID"
    val pointId         = "Point_ID"
    val analyte         = "Analyte"
    val analysisMethod  = "AnalysisMethod"
    val analysisDate    = "AnalysisDate"
    val units           = "Units"
    val labId           = "WCLab_ID"
    val analysesAgency  = "AnalysesAgency"

    /** Value of AnalysesAgency db column
      */
    val analysesAgencyDefault = "NMBGMR"

    /** Name of "chemistry sample info" table
      */
    val chemistrySampleInfo: String

    /** Name of "major chemistry" table
      */
    val majorChemistry: String

    /** Name of "minor chemistry" table
      */
    val minorChemistry: String

    /** Suffix used to mark total analyte values
      */
    val totalAnalyteSuffix = "(total)"

    /** Convert total or base (non-total) analyte string to total analyte string
      */
    def totalAnalyte(s: String): String =
      if (s.endsWith(totalAnalyteSuffix)) s
      else s + totalAnalyteSuffix

    /** Convert total or base analyte string to base analyte string
      */
    def baseAnalyte(s: String): String =
      s.stripSuffix(totalAnalyteSuffix)
  }

  /** DbInfo instance
    */
  val dbInfo: DbInfo

  /** Map from XLS "Param" values to DB "AnalysisMethod" values
    */
  val methodMap = Map(
      Params.alkalinity  -> "As CaCO3",
      Params.bicarbonate -> "Alkalinity as HCO3",
      Params.tds         -> "Calculation",
      Params.hardness    -> "As CaCO3")

  /** Map from XLS "Results_Units" values to DB "Units" values, by "Param"
    */
  val unitsMap = Map(
      Params.anions      -> "epm",
      Params.cations     -> "epm",
      Params.hardness    -> "mg/L",
      Params.percentDiff -> "%Diff",
      Params.pH          -> "pH",
      Params.conductance -> "µS/cm")

  /** Map from XLS "Param" value to associated DB table
    */
  lazy val chemistryTable = Map(
      Params.alkalinity     -> dbInfo.majorChemistry,
      Params.aluminum       -> dbInfo.minorChemistry,
      Params.anions         -> dbInfo.majorChemistry,
      Params.antimony       -> dbInfo.minorChemistry,
      Params.arsenic        -> dbInfo.minorChemistry,
      Params.barium         -> dbInfo.minorChemistry,
      Params.beryllium      -> dbInfo.minorChemistry,
      Params.bicarbonate    -> dbInfo.majorChemistry,
      Params.boron          -> dbInfo.minorChemistry,
      Params.bromide        -> dbInfo.minorChemistry,
      Params.cadmium        -> dbInfo.minorChemistry,
      Params.calcium        -> dbInfo.majorChemistry,
      Params.carbonate      -> dbInfo.majorChemistry,
      Params.cations        -> dbInfo.majorChemistry,
      Params.chloride       -> dbInfo.majorChemistry,
      Params.chromium       -> dbInfo.minorChemistry,
      Params.cobalt         -> dbInfo.minorChemistry,
      Params.copper         -> dbInfo.minorChemistry,
      Params.fluoride       -> dbInfo.minorChemistry,
      Params.hardness       -> dbInfo.majorChemistry,
      Params.iron           -> dbInfo.minorChemistry,
      Params.lead           -> dbInfo.minorChemistry,
      Params.lithium        -> dbInfo.minorChemistry,
      Params.magnesium      -> dbInfo.majorChemistry,
      Params.manganese      -> dbInfo.minorChemistry,
      Params.mercury        -> dbInfo.minorChemistry,
      Params.molybdenum     -> dbInfo.minorChemistry,
      Params.nickel         -> dbInfo.minorChemistry,
      Params.nitrate        -> dbInfo.minorChemistry,
      Params.nitrite        -> dbInfo.minorChemistry,
      Params.phosphate      -> dbInfo.minorChemistry,
      Params.percentDiff    -> dbInfo.majorChemistry,
      Params.potassium      -> dbInfo.majorChemistry,
      Params.selenium       -> dbInfo.minorChemistry,
      Params.siliconDioxide -> dbInfo.minorChemistry,
      Params.silicon        -> dbInfo.minorChemistry,
      Params.silver         -> dbInfo.minorChemistry,
      Params.sodium         -> dbInfo.majorChemistry,
      Params.conductance    -> dbInfo.majorChemistry,
      Params.strontium      -> dbInfo.minorChemistry,
      Params.sulfate        -> dbInfo.majorChemistry,
      Params.tds            -> dbInfo.majorChemistry,
      Params.thallium       -> dbInfo.minorChemistry,
      Params.thorium        -> dbInfo.minorChemistry,
      Params.tin            -> dbInfo.minorChemistry,
      Params.titanium       -> dbInfo.minorChemistry,
      Params.uranium        -> dbInfo.minorChemistry,
      Params.vanadium       -> dbInfo.minorChemistry,
      Params.zinc           -> dbInfo.minorChemistry,
      Params.pH             -> dbInfo.majorChemistry)

  /** Map from XLS "Param" values to list of "Test" values, in priority order
    */
  val testPriority = Map(
      // The order of list elements is from most preferred to least preferred
      Params.strontium ->
        List("""(?i) *trace +metal.*""".r, """(?i) *cation.*""".r),
      Params.bromide ->
        List("""(?i) *low +bromide *""".r, """(?i) *anions +by +ic *""".r)
    )

  /** Map from DB "Analyte" values to range of acceptable values derived from
    * drinking water quality standards.
    */
  val standards = Map(
      analytes(Params.aluminum)   -> ((0.0f, 0.05f)),
      analytes(Params.antimony)   -> ((0.0f, 0.006f)),
      analytes(Params.arsenic)    -> ((0.0f, 0.01f)),
      analytes(Params.barium)     -> ((0.0f, 2.0f)),
      analytes(Params.beryllium)  -> ((0.0f, 0.004f)),
      analytes(Params.boron)      -> ((0.0f, 7.0f)),
      analytes(Params.cadmium)    -> ((0.0f, 0.005f)),
      analytes(Params.chloride)   -> ((0.0f, 250.0f)),
      analytes(Params.chromium)   -> ((0.0f, 0.1f)),
      analytes(Params.copper)     -> ((0.0f, 1.0f)),
      analytes(Params.fluoride)   -> ((0.0f, 2.0f)),
      analytes(Params.hardness)   -> ((0.0f, 150.0f)),
      analytes(Params.iron)       -> ((0.0f, 0.3f)),
      analytes(Params.lead)       -> ((0.0f, 0.015f)),
      analytes(Params.manganese)  -> ((0.0f, 0.05f)),
      analytes(Params.mercury)    -> ((0.0f, 0.002f)),
      analytes(Params.nickel)     -> ((0.0f, 0.1f)),
      analytes(Params.nitrate)    -> ((0.0f, 45.0f)),
      analytes(Params.nitrite)    -> ((0.0f, 3.3f)),
      analytes(Params.selenium)   -> ((0.0f, 0.05f)),
      analytes(Params.silver)     -> ((0.0f, 0.1f)),
      analytes(Params.sodium)     -> ((0.0f, 200.0f)),
      analytes(Params.sulfate)    -> ((0.0f, 250.0f)),
      analytes(Params.tds)        -> ((0.0f, 500.0f)),
      analytes(Params.thallium)   -> ((0.0f, 0.0025f)),
      analytes(Params.uranium)    -> ((0.0f, 0.03f)),
      analytes(Params.zinc)       -> ((0.0f, 5.0f)),
      analytes(Params.pH)         -> ((6.5f, 8.5f)))
}
