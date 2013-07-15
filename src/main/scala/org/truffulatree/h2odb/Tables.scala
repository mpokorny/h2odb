package org.truffulatree.h2odb

object Tables {
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
    lead           -> "Pd",
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

  val method = Map(
    alkalinity  -> "as CaCO3",
    bicarbonate -> "Alkalinity as HCO3",
    tds         -> "Calculation",
    hardness    -> "as CaCO3")

  val units = Map(
    anions      -> "epm",
    cations     -> "epm",
    hardness    -> "mg/L",
    percentDiff -> "%Diff",
    pH          -> "pH",
    conductance -> "µS/cm"
  )
}
