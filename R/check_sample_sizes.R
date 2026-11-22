# File Metadata ################################################################
# Title: Triple check of sample sizes — raw NHANES files, derived datasets, every figure and table
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 21 Nov 2026
# Version: 2.0
#
# 1. Recount the analysis samples straight from the raw NHANES .xpt files, with code that is
#    deliberately independent of 01_data_wrangling.R.
# 2. Count the derived datasets used by the analyses (data/derived/*.rds).
# 3. Sum the stratum n's in every figure's source data and in Table 2, and compare each with
#    the sample it should come from: everyone with height and weight (non-DXA analyses) or
#    the DXA subset (DXA analyses).
#
# Output: results/stats/sample_size_check.csv. The script stops if any count disagrees.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

## 1. independent recount from the raw files ###################################
xpt <- function(file) {
  p <- fs::dir_ls(paths$raw, recurse = TRUE, regexp = paste0("(?i)/", file, "\\.xpt$"))
  foreign::read.xport(p[1])
}
man <- nhanes_manifest |> filter(cycle %in% analysis_cycles())

raw_anthro <- map_dfr(unique(man$cycle), function(cyc) {
  f <- man |> filter(cycle == cyc)
  d <- xpt(f$file[f$component == "DEMO"])[, c("SEQN", "RIAGENDR", "RIDAGEYR")]
  b <- xpt(f$file[f$component == "BMX"])
  b <- b[is.na(b$BMIWT) & is.na(b$BMIHT) & !is.na(b$BMXWT) & !is.na(b$BMXHT), "SEQN", drop = FALSE]
  merge(d, b, by = "SEQN")
})
raw_dxa_ids <- map(man$file[man$component == "DXX"], function(f) {
  x <- xpt(f)
  x <- x[x$DXAEXSTS %in% 1 & complete.cases(x[, c("DXDTOFAT", "DXDTOLE", "DXDTOBMC", "DXDTOPF")]), ]
  unique(x$SEQN)
}) |> unlist()

raw <- raw_anthro |>
  mutate(sex = if_else(RIAGENDR == 2, "Females", "Males"), dxa = SEQN %in% raw_dxa_ids)

raw_counts <- c(
  adults              = sum(raw$RIDAGEYR >= 21),
  youth               = sum(raw$RIDAGEYR < 21),
  adults_dxa          = sum(raw$RIDAGEYR >= 21 & raw$dxa),
  youth_dxa_8_20      = sum(raw$RIDAGEYR %in% 8:20 & raw$dxa),
  youth_dxa_8_20_F    = sum(raw$RIDAGEYR %in% 8:20 & raw$dxa & raw$sex == "Females"),
  youth_dxa_8_20_M    = sum(raw$RIDAGEYR %in% 8:20 & raw$dxa & raw$sex == "Males"),
  males_2_19          = sum(raw$RIDAGEYR %in% 2:19 & raw$sex == "Males")
)

## 2. derived datasets #########################################################
adults   <- read_result("adults")
kids     <- read_result("kids")
kids_dxa <- read_result("kids_dxa")

derived_counts <- c(
  adults           = nrow(adults),
  youth            = nrow(kids),
  adults_dxa       = sum(adults$has_dxa & !is.na(adults$pbf)),
  youth_dxa_8_20   = nrow(kids_dxa),
  youth_dxa_8_20_F = sum(kids_dxa$sex == "Females"),
  youth_dxa_8_20_M = sum(kids_dxa$sex == "Males"),
  males_2_19       = sum(kids$sex == "Males" & kids$age <= 19)
)

## 3. n's used in every figure and table #######################################
src <- function(fig) read_csv(fs::path(paths$out_source, paste0("SourceData_Fig", fig, ".csv")), show_col_types = FALSE)
n_sum <- function(d) sum(d$n, na.rm = TRUE)

f1  <- src(1);  f2 <- src(2); f3 <- src(3); f5 <- src(5); f6 <- src(6); f8 <- src(8); f9 <- src(9); f10 <- src(10)
t2  <- read_csv(fs::path(paths$out_tables, "Table2.csv"), show_col_types = FALSE)
youth_ages_3_19 <- sum(kids$age %in% 3:19)

usage <- tribble(
  ~figure,     ~panel_or_part,                        ~sample,           ~expected,                    ~observed,
  "Figure 1",  "A/B binned points",                   "adults",          derived_counts[["adults"]],   n_sum(filter(f1, panel == "A")),
  "Figure 2",  "A β, youth strata",                   "youth",           derived_counts[["youth"]],    n_sum(filter(f2, panel == "A", str_detect(cohort, "Youth"))),
  "Figure 2",  "A β, adult strata",                   "adults",          derived_counts[["adults"]],   n_sum(filter(f2, panel == "A", str_detect(cohort, "Adults"))),
  "Figure 2",  "B partial r, youth strata",           "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f2, panel == "B", str_detect(cohort, "Youth"))),
  "Figure 2",  "B partial r, adult strata",           "adults_dxa",      derived_counts[["adults_dxa"]], n_sum(filter(f2, panel == "B", str_detect(cohort, "Adults"))),
  "Figure 2",  "C %BF at BMI 25, youth strata",       "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f2, panel == "C", element == "estimate", str_detect(cohort, "Youth"))),
  "Figure 2",  "C %BF at BMI 25, adult strata",       "adults_dxa",      derived_counts[["adults_dxa"]], n_sum(filter(f2, panel == "C", element == "estimate", str_detect(cohort, "Adults"))),
  "Figure 3",  "A β, female + male strata",           "youth",           derived_counts[["youth"]],    n_sum(filter(f3, panel == "A", element == "estimate", sex != "All")),
  "Figure 3",  "A β, sex-adjusted strata",            "youth",           derived_counts[["youth"]],    n_sum(filter(f3, panel == "A", element == "estimate", sex == "All")),
  "Figure 3",  "B growth velocity (ages 3–19)",       "youth ages 3–19", youth_ages_3_19,              n_sum(filter(f3, panel == "B", element == "estimate")),
  "Figure 3",  "C r(BMI, height), by sex",            "youth",           derived_counts[["youth"]],    n_sum(filter(f3, panel == "C", element == "estimate", sex != "All")),
  "Figure 3",  "C r(BMI, height), pooled",            "youth",           derived_counts[["youth"]],    n_sum(filter(f3, panel == "C", element == "estimate", sex == "All")),
  "Figure 5",  "A r(height, %BF)",                    "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f5, panel == "A", element == "estimate")),
  "Figure 5",  "B %BF by height quartile",            "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f5, panel == "B", element == "estimate")),
  "Figure 6",  "A R² per index (BMI rows only)",      "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f6, panel == "A", facet == "BMI")),
  "Figure 6",  "B ΔR² (BMI − Benn rows only)",        "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f6, panel == "B", element == "estimate", str_detect(facet, "^BMI"))),
  "Figure 8",  "A adult curves (one row per stratum)","adults_dxa",      derived_counts[["adults_dxa"]], n_sum(distinct(filter(f8, panel == "A", element == "curve"), sex, stratum, n)),
  "Figure 8",  "B youth curves (one row per stratum)","youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(distinct(filter(f8, panel == "B", element == "curve"), sex, stratum, n)),
  "Figure 9",  "Hattori, youth strata",               "youth_dxa_8_20",  derived_counts[["youth_dxa_8_20"]], n_sum(filter(f9, str_detect(cohort, "Youth"))),
  "Figure 9",  "Hattori, adult strata",               "adults_dxa",      derived_counts[["adults_dxa"]], n_sum(filter(f9, str_detect(cohort, "Adults"))),
  "Figure 10", "A binned points (males 2–19)",        "males_2_19",      derived_counts[["males_2_19"]], n_sum(filter(f10, panel == "A")),
  "Table 2",   "female + male columns",               "youth",           derived_counts[["youth"]],    n_sum(filter(t2, sex != "Sex-adjusted")),
  "Table 2",   "sex-adjusted column",                 "youth",           derived_counts[["youth"]],    n_sum(filter(t2, sex == "Sex-adjusted"))
) |>
  mutate(match = expected == observed)

## report ######################################################################
datasets <- tibble(sample = names(raw_counts), raw_recount = unname(raw_counts),
                   derived = unname(derived_counts[names(raw_counts)])) |>
  mutate(match = raw_recount == derived)

out <- bind_rows(
  datasets |> transmute(check = "raw recount vs derived dataset", item = sample, expected = raw_recount, observed = derived, match),
  usage    |> transmute(check = paste(figure, "—", sample), item = panel_or_part, expected, observed, match)
)
write_excel_csv(out, fs::path(paths$out_stats, "sample_size_check.csv"))

print(datasets)
print(usage |> select(figure, panel_or_part, sample, expected, observed, match), n = Inf)

if (!all(out$match)) {
  cli::cli_abort("{sum(!out$match)} sample-size check{?s} failed; see {.file {fs::path(paths$out_stats, 'sample_size_check.csv')}}")
}
cli::cli_alert_success("All {nrow(out)} sample-size checks agree.")

sessioninfo::session_info()
