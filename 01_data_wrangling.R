# File Metadata ################################################################
# Title: Read, QC, merge and derive the NHANES analysis datasets
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : NHANES .xpt files under paths$raw (see 00_nhanes_fetch_data.R)
# Output: <paths$out_data>/
#           nhanes_all.rds      every participant with valid height and weight (ages 2–85)
#           adults.rds          age >= 21
#           kids.rds            age 2–20
#           kids_dxa.rds        age 8–20 with a valid whole-body DXA scan
#           data_dictionary.csv one row per variable
#
# QC rules (unchanged from v1 of this script):
#   - DXA: keep scans with DXAEXSTS == 1 ("complete") and no missing component;
#          average the 5 multiply imputed rows per participant in 1999–2006.
#   - Body measures: drop any record with a weight or height comment code
#          (BMIWT / BMIHT non-missing) or missing weight/height. Missing waist
#          circumference does NOT exclude anyone (changed in v2; DISCREPANCIES.md B16).
#   - One cycle per period: 2017–March 2020 pre-pandemic files by default
#          (opts$use_2017_2018_cycle in R/setup.R).

source("R/setup.R")

cli::cli_h1("NHANES data assembly")

## locate files ################################################################
if (!fs::dir_exists(paths$raw)) {
  cli::cli_abort(c("No NHANES data folder at {.file {paths$raw}}.",
                   "i" = "Run {.code Rscript 00_nhanes_fetch_data.R} first."))
}
xpt_files <-tibble(path = fs::dir_ls(paths$raw, recurse = TRUE, regexp = "(?i)\\.xpt$")) |>
  mutate(file = toupper(fs::path_ext_remove(fs::path_file(path))))

files <- nhanes_manifest |>
  filter(cycle %in% analysis_cycles()) |>
  left_join(xpt_files, by = join_by(file))

if (anyNA(files$path)) {
  cli::cli_abort(c("Missing NHANES files: {files$file[is.na(files$path)]}",
                   "i" = "Run {.code Rscript 00_nhanes_fetch_data.R} first."))
}

#' Read one component across cycles, keeping and renaming selected variables
#'
#' @param comp      Manifest component ("DEMO", "BMX" or "DXX").
#' @param vars      Named character vector: new_name = "NHANES_NAME".
#' @return A tibble with a `cycle` column plus the renamed variables.
read_component <- function(comp, vars) {
  f <- files |> filter(component == comp)
  cli::cli_inform(c("i" = "Reading {comp}: {nrow(f)} cycle{?s}"))
  map2_dfr(f$path, f$cycle, function(p, cyc) {
    foreign::read.xport(p) |>
      as_tibble() |>
      select(all_of(vars)) |>
      mutate(cycle = cyc, .after = 1)
  })
}

## Demographics ################################################################
demo <- read_component("DEMO", c(id = "SEQN", sex = "RIAGENDR", age = "RIDAGEYR",
                                 ethnicity = "RIDRETH1"))

## Body measures ###############################################################
body_raw <- read_component("BMX", c(id = "SEQN", weight_kg = "BMXWT", weight_comment = "BMIWT",
                                    height_cm = "BMXHT", height_comment = "BMIHT",
                                    waist_cm = "BMXWAIST"))

body <- body_raw |>
  filter(is.na(weight_comment), is.na(height_comment)) |>   # drop flagged measurements
  select(-weight_comment, -height_comment) |>
  filter(!is.na(weight_kg), !is.na(height_cm))               # waist may be missing (B16)

if (opts$v1_waist_exclusion) {                               # validation mode only
  cli::cli_warn("Validation mode: excluding participants with missing waist (v1 rule).")
  body <- body |> filter(!is.na(waist_cm))
}

## DXA whole body ##############################################################
dxa_raw <- read_component("DXX", c(id = "SEQN", dxa_status = "DXAEXSTS", fat_g = "DXDTOFAT",
                                   bone_g = "DXDTOBMC", lean_g = "DXDTOLE",
                                   pbf_nhanes = "DXDTOPF"))

dxa <- dxa_raw |>
  filter(dxa_status == 1) |>
  na.omit() |>
  group_by(id) |>                                            # average imputations (1999–2006)
  summarise(across(c(fat_g, bone_g, lean_g, pbf_nhanes), mean), .groups = "drop")

## merge #######################################################################
nhanes <- demo |>
  left_join(body, by = join_by(id, cycle)) |>
  left_join(dxa,  by = join_by(id)) |>
  distinct() |>
  filter(!is.na(weight_kg), !is.na(height_cm))

## derived variables ###########################################################
age_decade_lbl <- c("21–29", "30–39", "40–49", "50–59", "60–69", "70+")
age_5y_lbl     <- c("21–24", paste0(seq(25, 75, 5), "–", seq(29, 79, 5)), "80+")

nhanes <- nhanes |>
  mutate(
    sex          = recode_sex(sex),
    cohort       = if_else(age >= 21, "adults", "youth"),
    height_m     = height_cm / 100,
    # weight–stature indices
    bmi          = weight_kg / height_m^2,
    tmi          = weight_kg / height_m^3,
    log10_weight = log10(weight_kg),
    log10_height = log10(height_m),
    # DXA body composition (kg) and indices (kg/m²)
    has_dxa      = !is.na(fat_g),
    fat_kg       = fat_g  / 1000,
    lean_kg      = lean_g / 1000,                 # lean soft tissue, excludes bone mineral
    bone_kg      = bone_g / 1000,
    ffm_kg       = lean_kg + bone_kg,             # fat-free mass
    pbf          = 100 * fat_kg / (fat_kg + lean_kg + bone_kg),   # the single %BF variable
    fmi          = fat_kg  / height_m^2,
    ffmi         = ffm_kg  / height_m^2,
    # age strata
    age_decade   = cut(age, c(21, 30, 40, 50, 60, 70, Inf), age_decade_lbl, right = FALSE),
    age_5y       = cut(age, c(21, seq(25, 80, 5), Inf),     age_5y_lbl,     right = FALSE)
  )

## %BF check: computed vs NHANES-supplied (DISCREPANCIES Q2) ###################
pbf_chk <- nhanes |> filter(has_dxa) |>
  summarise(r = cor(pbf, pbf_nhanes), mean_diff = mean(pbf - pbf_nhanes),
            max_abs_diff = max(abs(pbf - pbf_nhanes)))
cli::cli_inform(c("i" = "%BF computed vs DXDTOPF: r = {round(pbf_chk$r, 6)}, mean diff = {signif(pbf_chk$mean_diff, 2)}, max |diff| = {round(pbf_chk$max_abs_diff, 2)}"))
nhanes <- nhanes |> select(-fat_g, -lean_g, -bone_g, -pbf_nhanes)

## analysis datasets ###########################################################
adults   <- nhanes |> filter(cohort == "adults")
kids     <- nhanes |> filter(cohort == "youth") |> select(-age_decade, -age_5y)
kids_dxa <- kids   |> filter(age >= 8, has_dxa)

## sample sizes ################################################################
counts <- tribble(
  ~quantity,                                   ~n,
  "All participants (valid height and weight)", nrow(nhanes),
  "Adults (>= 21 y)",                           nrow(adults),
  "Youth (2-20 y)",                             nrow(kids),
  "Youth 8-20 y with DXA, females",             sum(kids_dxa$sex == "Females"),
  "Youth 8-20 y with DXA, males",               sum(kids_dxa$sex == "Males"),
  "Adults with DXA",                            sum(adults$has_dxa),
  "Missing waist circumference (kept)",         sum(is.na(nhanes$waist_cm))
)
print(counts)
write_excel_csv(counts, fs::path(paths$out_stats, "sample_sizes.csv"))

## data dictionary #############################################################
dict <- tribble(
  ~variable,      ~description,
  "id",           "NHANES respondent sequence number (SEQN)",
  "cycle",        "NHANES cycle, e.g. 1999-2000; 2017-2020 = pre-pandemic files",
  "sex",          "Females / Males (RIAGENDR)",
  "age",          "Age in years at screening (RIDAGEYR; top-coded at 80 or 85 depending on cycle)",
  "ethnicity",    "RIDRETH1: 1 Mexican American, 2 Other Hispanic, 3 NH White, 4 NH Black, 5 Other/multiracial",
  "weight_kg",    "Body weight, kg (BMXWT)",
  "height_cm",    "Standing height, cm (BMXHT)",
  "waist_cm",     "Waist circumference, cm (BMXWAIST)",
  "cohort",       "adults (>= 21 y) or youth (< 21 y)",
  "height_m",     "Standing height, m",
  "bmi",          "Body mass index, kg/m^2",
  "tmi",          "Tri-ponderal mass index, kg/m^3",
  "log10_weight", "log10 of weight (kg)",
  "log10_height", "log10 of height (m)",
  "has_dxa",      "TRUE if a complete whole-body DXA scan is available",
  "fat_kg",       "DXA total fat mass, kg (DXDTOFAT / 1000; mean of implicates 1999-2006)",
  "lean_kg",      "DXA total lean soft tissue excluding bone mineral, kg (DXDTOLE / 1000)",
  "bone_kg",      "DXA total bone mineral content, kg (DXDTOBMC / 1000)",
  "ffm_kg",       "Fat-free mass = lean_kg + bone_kg",
  "pbf",          "Percent body fat = 100 * fat / (fat + lean + bone); the only %BF variable used",
  "fmi",          "Fat mass index, kg/m^2",
  "ffmi",         "Fat-free mass index, kg/m^2",
  "age_decade",   "Adult age strata: 21-29 ... 60-69, 70+ (adults only)",
  "age_5y",       "Adult 5-year strata: 21-24 ... 75-79, 80+ (adults only; Hattori chart)"
)
stopifnot(setequal(dict$variable, names(nhanes)))
write_excel_csv(dict, fs::path(paths$out_data, "data_dictionary.csv"))

## save ########################################################################
cli::cli_h2("Saving analysis datasets")
save_result(nhanes,   "nhanes_all")
save_result(adults,   "adults")
save_result(kids,     "kids")
save_result(kids_dxa, "kids_dxa")

cli::cli_h1("Data wrangling complete")

## session info ################################################################
sessioninfo::session_info()
