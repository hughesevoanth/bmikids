# File Metadata ################################################################
# Title: Combine the per-figure source data into one Excel workbook
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : results/source_data/SourceData_Fig1.csv ... SourceData_Fig11.csv
# Output: results/source_data/SourceData_AllFigures.xlsx
#         sheet "README" (figure titles + a description of every column), then one sheet per figure
#
# The script stops if a CSV contains a column that has no description below, so the README
# can never fall out of step with the data.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")
library(openxlsx)

## figure titles ###############################################################
fig_titles <- c(
  `1`  = "Adult weight–height allometry: log–log model (A) and back-transformed model (B), adults ≥ 21 y",
  `2`  = "The three BMI assumptions across the life course, sex-adjusted: β (A), partial r of height with %BF (B), %BF at BMI 25 (C)",
  `3`  = "Youth 2–20 y: allometric exponent β (A), growth velocity (B), Pearson r of BMI with height (C)",
  `4`  = "Analytic height-dependent bias in BMI, (H/H0)^(β−2) − 1, at β = 2 and the Table 2 β for ages 3, 5 and 11",
  `5`  = "Youth 8–20 y (DXA): Pearson r of height with %BF (A); mean %BF by height quartile (B)",
  `6`  = "Youth 8–20 y (DXA): R² of %BF with BMI, TMI and Benn's index (A); ΔR² vs Benn (B); ΔR² vs height–%BF r (C)",
  `7`  = "Youth 2–20 y: GAM smooths of height and weight (A) and BMI, TMI and Benn's index (B) by age and sex",
  `8`  = "Compositional stability: %BF vs BMI curves by age stratum in adults (A) and youth (B); drift in %BF at fixed BMI (C)",
  `9`  = "Hattori chart: mean fat-free mass index vs fat mass index by sex and age stratum, youth and adults (DXA)",
  `10` = "Deriving BMI-for-age percentiles, males 2–19 y: BMI by age (A, binned counts), empirical percentiles (B), GAM-smoothed percentiles (C)",
  `11` = "CDC 2022 BMI-for-age reference: L, M and S by age and sex (A–C); percentile curves incl. 120% of the 95th (D)"
)

## column dictionary ###########################################################
col_dict <- tribble(
  ~column,        ~description,
  "panel",        "Figure panel letter (A, B, C, D)",
  "element",      "What the row is: estimate (point), smooth / line_vertex / curve / fitted_line (a vertex of a drawn line), bin_count (count of participants in a grid cell, used instead of individual points), reference_line, linear_fit, drift, stratum_mean, empirical_percentile, pbf_at_bmi25",
  "x",            "x position actually plotted (see x_variable or the figure title for its meaning; for age axes this is the age or the median age of the stratum, including any small horizontal offset used to separate sexes)",
  "y",            "Value plotted on the y-axis (see y_variable)",
  "x_variable",   "Name and unit of x",
  "y_variable",   "Name and unit of y",
  "lo",           "Lower bound of the whisker / ribbon (95% CI unless `note` says otherwise)",
  "hi",           "Upper bound of the whisker / ribbon (95% CI unless `note` says otherwise)",
  "n",            "Number of participants contributing to the estimate or grid cell (Fig 8C: number of age strata contributing)",
  "p",            "Two-sided p-value for r = 0",
  "star",         "TRUE where p < 0.05 (asterisk drawn)",
  "sex",          "Females, Males, or All (sexes pooled)",
  "cohort",       "Adults (21+ y), Youth (<21 y), or Youth (8–20 y) for DXA analyses",
  "stratum",      "Age stratum: single year of age for youth; decade (21–29 … 70+) or 5-year band (21–24 … 80+) for adults",
  "age",          "Age in years (single-year stratum)",
  "age_mid",      "Median age of participants in the stratum",
  "group",        "Grouping within a panel: sex (Fig 5A) or height quartile within age × sex (Fig 5B)",
  "facet",        "Sub-panel: index (BMI, TMI, Benn) or comparison (BMI − Benn, TMI − Benn)",
  "measure",      "Measure smoothed (height, weight, BMI, TMI, Benn's index) with unit",
  "percentile",   "Percentile of BMI-for-age (3rd … 97th; 120% of 95th)",
  "parameter",    "CDC LMS parameter (L = Box-Cox power, M = median, S = coefficient of variation) or BMI percentile",
  "agemos",       "Age in months as tabulated by CDC (x = agemos / 12)",
  "stage",        "Growth stage used for the analytic bias curve",
  "beta",         "Allometric exponent β used for the curve",
  "label",        "Legend label",
  "full_support", "TRUE when every age stratum of that cohort × sex contributes at this reference BMI (solid line in Fig 8C)",
  "note",         "Meaning of lo/hi or y for this element",
  "bmi",          "Mean BMI (kg/m²) of the stratum",
  "pbf",          "Mean percent body fat (DXA) of the stratum"
)

## read every figure's CSV #####################################################
csvs <- fs::dir_ls(paths$out_source, regexp = "SourceData_Fig[0-9]+\\.csv$")
figs <- str_extract(fs::path_file(csvs), "[0-9]+") |> as.integer()
csvs <- csvs[order(figs)]; figs <- sort(figs)
sheets <- map(csvs, read_csv, show_col_types = FALSE) |> set_names(paste0("Fig", figs))

undocumented <- setdiff(unique(unlist(map(sheets, names))), col_dict$column)
if (length(undocumented)) cli::cli_abort("Undocumented source-data columns: {undocumented}")

## README sheet ################################################################
readme_top <- tibble(
  item = c("Title", "Data", "Individual-level data",
           "Regenerating", "Survey weights", "Generated"),
  description = c(
    "Source data for: Body Mass Index: Mathematical Foundation of the World's Most Widely Used Adiposity Index in Pediatrics",
    "NHANES 1999–2023 public-use files (anthropometry); whole-body DXA from 1999–2006 and 2011–2016; CDC 2022 BMI-for-age LMS table (Figure 11)",
    "None. Panels that show individuals (Fig 1, Fig 10A) are given as counts in fine grid cells (element = bin_count). The individual view can be rebuilt from public NHANES files with the code in this repository (00_nhanes_fetch_data.R, 01_data_wrangling.R).",
    "Every value in this workbook is written by the figure scripts in figures/, which then draw the figure from the same CSV.",
    "None used; all estimates are unweighted.",
    format(Sys.time(), "%Y-%m-%d %H:%M %Z")
  )
)
readme_figs <- tibble(sheet = names(sheets), title = fig_titles[as.character(figs)],
                      rows = map_int(sheets, nrow), columns = map_chr(sheets, ~ paste(names(.x), collapse = ", ")))
readme_cols <- col_dict |> filter(column %in% unique(unlist(map(sheets, names))))

## write workbook ##############################################################
wb <- createWorkbook()
bold <- createStyle(textDecoration = "bold")
addWorksheet(wb, "README")
writeData(wb, "README", readme_top, startRow = 1, headerStyle = bold)
writeData(wb, "README", readme_figs, startRow = nrow(readme_top) + 3, headerStyle = bold)
writeData(wb, "README", readme_cols, startRow = nrow(readme_top) + nrow(readme_figs) + 6, headerStyle = bold)
setColWidths(wb, "README", cols = 1:4, widths = c(22, 110, 8, 60))
iwalk(sheets, function(d, nm) {
  addWorksheet(wb, nm)
  writeData(wb, nm, d, headerStyle = bold)
  freezePane(wb, nm, firstRow = TRUE)
})
out <- fs::path(paths$out_source, "SourceData_AllFigures.xlsx")
saveWorkbook(wb, out, overwrite = TRUE)
cli::cli_alert_success("Wrote {.file {out}}: README + {length(sheets)} figure sheets")

sessioninfo::session_info()
