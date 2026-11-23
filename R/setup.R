# File Metadata ################################################################
# Title: Shared setup — packages, paths, NHANES file manifest, plot theme, helpers
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Every other script starts with `source("R/setup.R")`. Run all scripts with the
# working directory set to the repository root (opening `bmikids.Rproj` does this).
#
# Contents
#   1. Packages
#   2. Paths            (override with env vars BMIKIDS_DATA / BMIKIDS_RESULTS)
#   3. Analysis options (one place for every switch)
#   4. NHANES file manifest (what 00_nhanes_fetch_data.R downloads, 01 reads)
#   5. Labels, palettes, theme, scales, export helpers
#   6. Small shared helpers (more are added as the analysis scripts are built)

## 1. Packages #################################################################
suppressPackageStartupMessages({
  library(tidyverse)
  library(cli)
  library(fs)
  library(patchwork)
  library(ggtext)
  library(mgcv)
})

## 2. Paths ####################################################################
#' Locate the repository root
#'
#' Walks up from the working directory until it finds `R/setup.R`, so scripts in
#' `figures/` or `tables/` work whether they are run from the root or their own folder.
#'
#' @return Absolute path to the repository root.
find_repo_root <- function(start = getwd()) {
  d <- fs::path_abs(start)
  while (!fs::file_exists(fs::path(d, "R/setup.R"))) {
    if (d == fs::path_dir(d)) cli::cli_abort("Could not find R/setup.R above {start}.")
    d <- fs::path_dir(d)
  }
  d
}

repo_dir <- find_repo_root()

# Defaults live inside the repository (<repo>/data, <repo>/results), so a fresh clone
# works with no set-up. To keep data or results elsewhere, set BMIKIDS_DATA /
# BMIKIDS_RESULTS, e.g. in a `.Renviron` file in the repository root (git-ignored; see README).
#' Resolve a path from an env var; relative paths are taken relative to the repo root
env_path <- function(var, default) {
  p <- Sys.getenv(var, unset = default)
  if (!fs::is_absolute_path(p)) p <- fs::path(repo_dir, p)
  fs::path_norm(p)
}
data_dir    <- env_path("BMIKIDS_DATA",    "data")
results_dir <- env_path("BMIKIDS_RESULTS", "results")
derived_dir <- env_path("BMIKIDS_DERIVED", fs::path(data_dir, "derived"))

paths <- list(
  repo        = repo_dir,
  data        = data_dir,
  raw         = fs::path(data_dir, "raw"),           # NHANES .xpt files live anywhere below here
  cdc_lms     = fs::path(data_dir, "bmi-age-2022.csv"),
  out         = results_dir,
  out_data    = derived_dir,                          # analysis datasets + intermediate results (.rds)
  out_figures = fs::path(results_dir, "figures"),     # FigureN.pdf / .png
  out_source  = fs::path(results_dir, "source_data"), # SourceData_FigN.csv + workbook
  out_tables  = fs::path(results_dir, "tables"),      # Table 2
  out_stats   = fs::path(results_dir, "stats")        # sample sizes, in-text numbers, session info
)

# create output folders on first use (only these folders; nothing else is touched)
walk(paths[str_starts(names(paths), "out_")], fs::dir_create)

## 3. Analysis options #########################################################
opts <- list(
  # Use 2017–2018 (DEMO_J/BMX_J/DXX_J) instead of 2017–March 2020 pre-pandemic (P_*) files.
  # FALSE reproduces the manuscript. TRUE recovers ~3,600 DXA scans that cannot be linked
  # to the P_ files (they use new SEQNs), at the cost of the 2019–2020 participants.
  # See DISCREPANCIES.md, B1 / Q4.
  use_2017_2018_cycle = as.logical(Sys.getenv("BMIKIDS_USE_2017_2018", unset = "FALSE")),
  # Validation only: re-apply the v1 exclusion of participants with missing waist
  # circumference, to show the v2 code reproduces the manuscript. FALSE for all results.
  v1_waist_exclusion = as.logical(Sys.getenv("BMIKIDS_V1_WAIST_EXCLUSION", unset = "FALSE")),
  min_stratum_n = 100,      # skip strata with fewer participants
  ref_bmi       = 25,       # reference BMI for compositional stability
  ref_bmi_sweep = 20:35,    # reference BMIs for the drift sweep
  ref_bmi_tol   = 0.5,      # curve must reach within this of the reference BMI
  n_boot        = 2000,     # bootstrap resamples
  seed          = 1,
  p_star        = 0.05      # asterisk threshold used in every figure
)

## 4. NHANES file manifest #####################################################
# One row per file. URL pattern verified against the original fetch log (Nov 2025).
nhanes_manifest <- tribble(
  ~cycle,       ~component, ~file,
  "1999-2000",  "DEMO",     "DEMO",
  "2001-2002",  "DEMO",     "DEMO_B",
  "2003-2004",  "DEMO",     "DEMO_C",
  "2005-2006",  "DEMO",     "DEMO_D",
  "2007-2008",  "DEMO",     "DEMO_E",
  "2009-2010",  "DEMO",     "DEMO_F",
  "2011-2012",  "DEMO",     "DEMO_G",
  "2013-2014",  "DEMO",     "DEMO_H",
  "2015-2016",  "DEMO",     "DEMO_I",
  "2017-2018",  "DEMO",     "DEMO_J",
  "2017-2020",  "DEMO",     "P_DEMO",
  "2021-2023",  "DEMO",     "DEMO_L",
  "1999-2000",  "BMX",      "BMX",
  "2001-2002",  "BMX",      "BMX_B",
  "2003-2004",  "BMX",      "BMX_C",
  "2005-2006",  "BMX",      "BMX_D",
  "2007-2008",  "BMX",      "BMX_E",
  "2009-2010",  "BMX",      "BMX_F",
  "2011-2012",  "BMX",      "BMX_G",
  "2013-2014",  "BMX",      "BMX_H",
  "2015-2016",  "BMX",      "BMX_I",
  "2017-2018",  "BMX",      "BMX_J",
  "2017-2020",  "BMX",      "P_BMX",
  "2021-2023",  "BMX",      "BMX_L",
  "1999-2000",  "DXX",      "DXX",     # 1999–2006: 5 multiply imputed rows per participant
  "2001-2002",  "DXX",      "DXX_B",
  "2003-2004",  "DXX",      "DXX_C",
  "2005-2006",  "DXX",      "DXX_D",
  "2011-2012",  "DXX",      "DXX_G",   # no whole-body DXA in 2007–2010 or 2021–2023
  "2013-2014",  "DXX",      "DXX_H",
  "2015-2016",  "DXX",      "DXX_I",
  "2017-2018",  "DXX",      "DXX_J"
) |>
  mutate(start_year = str_sub(cycle, 1, 4),
         url = str_glue("https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/{start_year}/DataFiles/{file}.xpt"))

#' Cycles used in the analysis, given `opts$use_2017_2018_cycle`
analysis_cycles <- function(use_2017_2018 = opts$use_2017_2018_cycle) {
  all <- unique(nhanes_manifest$cycle)
  if (use_2017_2018) setdiff(all, "2017-2020") else setdiff(all, "2017-2018")
}

## 5. Labels, palettes, theme, scales, export ##################################
lbl_sex    <- c(f = "Females", m = "Males")
lbl_cohort <- c(adults = "Adults (21+ y)", youth = "Youth (<21 y)", youth_dxa = "Youth (8–20 y)")

pal_sex    <- c(Females = "grey50", Males = "grey80", All = "black")    # females always darker
pal_cohort <- set_names(c("#004488", "#D55E00", "#D55E00"), lbl_cohort)

#' Grey ramp for percentile curves (RColorBrewer Greys 3–9, interpolated)
pal_percentile <- function(n) colorRampPalette(RColorBrewer::brewer.pal(9, "Greys")[3:9])(n)

#' Recode NHANES sex codes (1 = male, 2 = female) to the labels used everywhere
recode_sex <- function(x) {
  factor(dplyr::recode(as.character(x), `1` = "Males", `2` = "Females"),
         levels = c("Females", "Males"))
}

#' Shared ggplot theme: theme_bw() with every piece of text in bold
theme_bmikids <- function(base_size = 11) {
  theme_bw(base_size = base_size) +
    theme(text        = element_text(face = "bold"),
          axis.text   = element_text(face = "bold", colour = "black"),
          legend.text = element_text(face = "bold"),
          strip.text  = element_text(face = "bold"),
          plot.tag    = element_text(face = "bold"))
}

#' Stretch ages below `cut` by a factor `k` on a continuous axis
#'
#' @param cut Age at which the stretch stops (youth–adult divider).
#' @param k   Stretch factor for ages <= cut.
stretch_youth <- function(cut = 21, k = 2) {
  scales::trans_new(
    "stretch_youth",
    transform = function(x) ifelse(x <= cut, x * k, cut * k + (x - cut)),
    inverse   = function(y) ifelse(y <= cut * k, y / k, y - cut * k + cut)
  )
}

#' Life-course x scale shared by every stacked age panel
#'
#' @param limits Age limits; pass the same value to every panel so they align.
scale_x_lifecourse <- function(limits, ...) {
  scale_x_continuous(transform    = stretch_youth(k = 2),
                     limits       = limits,
                     oob          = scales::oob_keep,
                     breaks       = c(seq(2, 20, 2), seq(30, 80, 10)),
                     minor_breaks = c(seq(3, 19, 2), seq(25, 75, 10)),
                     expand       = expansion(add = c(1.5, 3)),
                     ...)
}

#' Save a figure as PDF and EPS (vector, cairo) and as TIFF and PNG (600 dpi, ragg)
#'
#' EPS cannot hold transparency: semi-transparent layers are drawn as 600-dpi images
#' inside the EPS (`fallback_resolution`). TIFF uses lossless LZW compression.
#'
#' @param plot   ggplot / patchwork object.
#' @param name   File stem, e.g. "Figure1".
#' @param width,height Size in inches.
save_figure <- function(plot, name, width, height) {
  # Render to a local temporary file first, check it is non-empty, then copy into place.
  # Writing straight into a synced folder (e.g. OneDrive) while a viewer has the file
  # open can leave a 0-byte file without any error.
  write_checked <- function(ext, device, ...) {
    tmp  <- tempfile(fileext = ext)
    dest <- fs::path(paths$out_figures, paste0(name, ext))
    ggsave(tmp, plot, width = width, height = height, device = device, ...)
    if (!fs::file_exists(tmp) || fs::file_size(tmp) == 0) cli::cli_abort("Rendering {.file {dest}} failed.")
    fs::file_copy(tmp, dest, overwrite = TRUE)
    if (fs::file_size(dest) != fs::file_size(tmp)) cli::cli_abort("Copy to {.file {dest}} is incomplete; close any viewer and re-run.")
    fs::file_delete(tmp)
    dest
  }
  files <- c(
    pdf  = write_checked(".pdf",  cairo_pdf),                                  # vector; for co-authors
    eps  = write_checked(".eps",  cairo_ps, fallback_resolution = 600),        # vector; journal submission
    # raster sizes are set by ggsave()'s `dpi` (its default of 300 overrides a device `res`)
    tiff = write_checked(".tiff", ragg::agg_tiff, dpi = 600, compression = "lzw"),  # journal submission
    png  = write_checked(".png",  ragg::agg_png,  dpi = 600)                   # quick viewing
  )
  sizes <- paste(sprintf("%s %s", toupper(names(files)), format(fs::file_size(files))), collapse = ", ")
  cli::cli_alert_success("Saved {name}: {sizes}")
  invisible(files)
}

#' Write the source data for a figure, then read it back for plotting
#'
#' Plotting from the re-read CSV guarantees the published source data are exactly
#' what is drawn.
#'
#' @param df  Aggregate data (no individual-level rows).
#' @param fig Figure number, e.g. 1 or "2".
#' @return The data as read back from disk.
write_source_data <- function(df, fig) {
  f <- fs::path(paths$out_source, paste0("SourceData_Fig", fig, ".csv"))
  readr::write_excel_csv(df, f, na = "")
  cli::cli_alert_success("Wrote {.file {f}} ({nrow(df)} rows)")
  readr::read_csv(f, show_col_types = FALSE)
}

## 6. Small shared helpers #####################################################
#' Read a results object written by 01–04
read_result <- function(name) readRDS(fs::path(paths$out_data, paste0(name, ".rds")))

#' Save a results object for later scripts
save_result <- function(x, name) {
  f <- fs::path(paths$out_data, paste0(name, ".rds"))
  saveRDS(x, f)
  cli::cli_alert_success("Saved {.file {f}}")
  invisible(f)
}

#' Record a reproduced in-text number
#'
#' Values accumulate in `.stats` and are written by `save_stats()`. Each `key` matches a
#' row of `stats/manuscript_values.csv`, which holds the quoted value and its location.
#'
#' @param key    Short identifier, e.g. "adult_beta_sexadj".
#' @param value  Reproduced numeric value (unrounded).
#' @param note   Optional free text (e.g. how the value was derived).
.stats <- tibble(key = character(), reproduced_value = double(), note = character())
record_stat <- function(key, value, note = "") {
  .stats <<- .stats |>
    filter(key != !!key) |>
    add_row(key = key, reproduced_value = as.double(value), note = note)
  invisible(value)
}

#' Write the recorded stats for one script to results/stats/reproduced_<script>.csv
save_stats <- function(script) {
  f <- fs::path(paths$out_stats, paste0("reproduced_", script, ".csv"))
  write_excel_csv(mutate(.stats, source_script = script), f)
  cli::cli_alert_success("Recorded {nrow(.stats)} in-text values in {.file {f}}")
  invisible(f)
}

## 7. Analysis helpers #########################################################

#' Extract the fitted equation from a linear model as a character string
#'
#' @param model A fitted linear model object (e.g., from lm()).
#' @param digits Number of decimal places for coefficients (trailing zeros kept).
#' @param eq_sign Symbol between outcome and predictors (default "=").
#' @param outcome_label Optional display label for the outcome (e.g., "log10(Ŵ)").
#' @param labels Optional named vector mapping coefficient names to display labels,
#'   e.g., c(sexMales = "sex M", log10_height = "log10 H").
#' @param unicode Use true minus (−) and multiplication (×) signs? Default TRUE.
#' @param print Print the equation to the console? Default TRUE.
#'
#' @return The equation as a character string (invisibly).
extract_formula <- function(model, digits = 3, eq_sign = "=",
                            outcome_label = NULL, labels = NULL,
                            unicode = TRUE, print = TRUE) {
  outcome <- if (is.null(outcome_label)) deparse(formula(model)[[2]]) else outcome_label
  betas <- coef(model)
  minus <- if (unicode) "−" else "-"
  times <- if (unicode) "×" else "*"
  fmt <- function(x) formatC(abs(x), format = "f", digits = digits)
  lab <- function(v) if (!is.null(labels) && v %in% names(labels)) labels[[v]] else v
  terms <- vapply(names(betas), function(nm) {
    if (nm == "(Intercept)") fmt(betas[[nm]]) else paste(fmt(betas[[nm]]), times, lab(nm))
  }, character(1))
  neg <- betas < 0
  rhs <- paste0(if (neg[1]) minus else "", terms[1])         # first term: leading minus only
  if (length(terms) > 1) {
    seps <- ifelse(neg[-1], paste0(" ", minus, " "), " + ")
    rhs  <- paste0(rhs, paste0(seps, terms[-1], collapse = ""))
  }
  formula_string <- paste(outcome, eq_sign, rhs)
  if (print) cat(formula_string, "\n")
  invisible(formula_string)
}

#' Share of variance explained by the last term of a sequential (type I) model
#'
#' With `y ~ sex + x`, this is ΔR² for `x` beyond `sex`.
#' @return Numeric ΔR².
delta_r2_last <- function(model) {
  ss <- anova(model)[["Sum Sq"]]
  ss[length(ss) - 1] / sum(ss)                                # last predictor, before residuals
}

#' Allometric exponent β (log10 W ~ log10 H) within strata
#'
#' @param df       Data with `log10_weight`, `log10_height`, `sex`, `age` and the stratum column.
#' @param stratum  Name of the stratum column (string).
#' @param adjust_sex FALSE: one fit per sex × stratum. TRUE: one fit per stratum with sex
#'   as a covariate, plus the p-value for a height × sex interaction (`p_int`).
#' @param min_n    Skip strata with fewer participants.
#' @return Tibble: stratum, [sex], n, age_mid (median age), beta, se, lo, hi (95% CI), r2, [p_int].
beta_by_stratum <- function(df, stratum, adjust_sex = FALSE, min_n = opts$min_stratum_n) {
  groups <- if (adjust_sex) stratum else c("sex", stratum)
  df |>
    filter(!is.na(log10_weight), !is.na(log10_height), !is.na(sex)) |>
    group_by(across(all_of(groups))) |>
    group_modify(.keep = TRUE, function(d, key) {   # keep the stratum column (it may be `age`)
      if (nrow(d) < min_n || (adjust_sex && n_distinct(d$sex) < 2)) return(tibble())
      m  <- if (adjust_sex) lm(log10_weight ~ log10_height + sex, data = d)
            else            lm(log10_weight ~ log10_height, data = d)
      s  <- summary(m)$coefficients["log10_height", ]
      ci <- confint(m)["log10_height", ]
      out <- tibble(n = nrow(d), age_mid = median(d$age),
                    beta = s[["Estimate"]], se = s[["Std. Error"]],
                    lo = ci[[1]], hi = ci[[2]], r2 = summary(m)$r.squared)
      if (adjust_sex) {
        out$p_int <- anova(m, update(m, . ~ . + log10_height:sex))$`Pr(>F)`[2]
      }
      out
    }) |>
    ungroup()
}

#' Pearson correlation summary for one group (r, 95% CI, p, n)
#'
#' Rows with a missing `x` or `y` are dropped first, so `n` is the number used.
#' @param x,y Column names (strings).
cor_summary <- function(df, x, y) {
  d <- df |> filter(!is.na(.data[[x]]), !is.na(.data[[y]]))
  n <- nrow(d)
  if (n < 4) return(tibble(r = NA_real_, lo = NA_real_, hi = NA_real_, p = NA_real_, n = n))
  ct <- cor.test(d[[x]], d[[y]])
  tibble(r = unname(ct$estimate), lo = ct$conf.int[1], hi = ct$conf.int[2], p = ct$p.value, n = n)
}

#' Sex-adjusted (partial) correlation of height with %BF within strata
#'
#' Height and %BF are each residualised on sex; r is the correlation of the residuals.
#' Fisher-z 95% CI with SE = 1 / sqrt(n − 4) (one covariate).
#' @return Tibble: stratum, n, age_mid, r, lo, hi, pp_per_10cm, p_int.
hf_corr <- function(df, stratum, min_n = opts$min_stratum_n) {
  df |>
    filter(!is.na(height_cm), !is.na(pbf), !is.na(sex)) |>
    group_by(across(all_of(stratum))) |>
    group_modify(.keep = TRUE, function(d, key) {   # keep the stratum column (it may be `age`)
      if (nrow(d) < min_n || n_distinct(d$sex) < 2) return(tibble())
      r  <- cor(resid(lm(height_cm ~ sex, data = d)), resid(lm(pbf ~ sex, data = d)))
      ci <- tanh(atanh(r) + c(-1, 1) * qnorm(0.975) / sqrt(nrow(d) - 4))
      m  <- lm(pbf ~ height_cm + sex, data = d)
      tibble(n = nrow(d), age_mid = median(d$age), r = r, lo = ci[1], hi = ci[2],
             pp_per_10cm = 10 * coef(m)[["height_cm"]],          # %BF points per 10 cm
             p_int = anova(m, update(m, . ~ . + height_cm:sex))$`Pr(>F)`[2])
    }) |>
    ungroup()
}

#' GAM curves of %BF on BMI within strata
#'
#' One `gam(pbf ~ s(bmi, k))` per sex × stratum (`adjust_sex = FALSE`), or one
#' `gam(pbf ~ s(bmi, k) + sex)` per stratum (`adjust_sex = TRUE`). In the adjusted version
#' the curve is the 50/50 average of the female and male predictions, with SEs from the
#' lpmatrix, and `p_int` tests whether the BMI curve differs by sex.
#'
#' Two kinds of rows are returned (`type`):
#' - "curve": 100 grid points spanning the 5th–95th BMI percentiles of the stratum
#'   (for plotting);
#' - "ref":   predictions at exactly each reference BMI in `ref_bmis`, kept only when the
#'   reference lies within `tol` of the stratum's 5th–95th BMI range (read by `pbf_at_ref()`).
#'
#' @return Tibble: stratum, [sex], type, bmi, fit, se, sd_resid, n, age_mid, [p_int].
#'   `n` counts only participants used in the fit.
fit_stratum_curves <- function(df, stratum, adjust_sex = FALSE, k = 5,
                               ref_bmis = opts$ref_bmi_sweep, tol = opts$ref_bmi_tol,
                               min_n = opts$min_stratum_n, n_grid = 100) {
  groups <- if (adjust_sex) stratum else c("sex", stratum)
  df |>
    filter(!is.na(pbf), !is.na(bmi), !is.na(sex)) |>
    group_by(across(all_of(groups))) |>
    group_modify(.keep = TRUE, function(d, key) {   # keep the stratum column (it may be `age`)
      if (nrow(d) < min_n || (adjust_sex && n_distinct(d$sex) < 2)) return(tibble())
      rng  <- quantile(d$bmi, c(0.05, 0.95))
      grid <- bind_rows(
        tibble(type = "curve", bmi = seq(rng[[1]], rng[[2]], length.out = n_grid)),
        tibble(type = "ref",   bmi = ref_bmis[ref_bmis >= rng[[1]] - tol & ref_bmis <= rng[[2]] + tol])
      )
      base <- tibble(n = nrow(d), age_mid = median(d$age))
      if (!adjust_sex) {
        m  <- gam(pbf ~ s(bmi, k = k), data = d)
        pr <- predict(m, newdata = grid, se.fit = TRUE)
        return(bind_cols(grid, fit = as.numeric(pr$fit), se = as.numeric(pr$se.fit),
                         sd_resid = sqrt(m$sig2), base))
      }
      d     <- mutate(d, sex = droplevels(sex))
      m     <- gam(pbf ~ s(bmi, k = k) + sex, data = d)
      m_int <- gam(pbf ~ s(bmi, by = sex, k = k) + sex, data = d)
      # equal-weight average of the female and male linear predictors
      X <- Reduce(`+`, lapply(levels(d$sex), function(s)
        0.5 * predict(m, type = "lpmatrix",
                      newdata = mutate(grid, sex = factor(s, levels = levels(d$sex))))))
      bind_cols(grid,
                fit = drop(X %*% coef(m)),
                se  = sqrt(rowSums((X %*% vcov(m)) * X)),
                sd_resid = sqrt(m$sig2), base,
                p_int = anova(m, m_int, test = "F")$`Pr(>F)`[2])
    }) |>
    ungroup()
}

#' %BF at reference BMIs from `fit_stratum_curves()` output
#'
#' Uses the exact predictions at each reference BMI (`type == "ref"`); strata whose BMI
#' range does not reach the reference (within `tol`) have no row.
#' @return Tibble: grouping columns, ref_bmi, pbf, lo, hi (95% CI of the conditional mean),
#'   se, sd_resid, n, age_mid, [p_int].
pbf_at_ref <- function(curves, ref_bmis = opts$ref_bmi) {
  curves |>
    filter(type == "ref", bmi %in% ref_bmis) |>
    mutate(ref_bmi = bmi, pbf = fit, lo = fit - 1.96 * se, hi = fit + 1.96 * se) |>
    select(-type, -bmi, -fit)
}

#' GAM smooth of y on x within groups, returned as curve vertices
#'
#' Used by figure scripts so the smooth that is drawn is also written to the source data.
#' @param df     Data with the x, y and grouping columns.
#' @param x,y    Column names (strings).
#' @param group  Grouping column names (character vector, may be empty).
#' @param bs     Spline basis (default thin-plate "tp", used in every figure).
#' @param k      Basis dimension (NULL = mgcv default, capped at the number of unique x).
#' @param n      Number of vertices per curve.
#' @return Tibble: group columns, x, y (fitted), element = "smooth".
smooth_by <- function(df, x, y, group = character(), bs = "tp", k = NULL, n = 200) {
  df |>
    filter(!is.na(.data[[x]]), !is.na(.data[[y]])) |>
    group_by(across(all_of(group))) |>
    group_modify(.keep = TRUE, function(d, key) {   # keep the stratum column (it may be `age`)
      kk <- if (is.null(k)) min(10, n_distinct(d[[x]]) - 1) else k
      m  <- gam(reformulate(sprintf("s(%s, bs = '%s', k = %d)", x, bs, kk), y), data = d)
      g  <- tibble(!!x := seq(min(d[[x]]), max(d[[x]]), length.out = n))
      tibble(x = g[[x]], y = as.numeric(predict(m, newdata = g)))
    }) |>
    ungroup() |>
    mutate(element = "smooth")
}

#' Bootstrap mean with a 95% percentile CI
#' @return Tibble: mean, se_boot, lo, hi, n.
boot_mean_ci <- function(x, R = opts$n_boot) {
  x <- x[!is.na(x)]
  if (length(x) < 2) return(tibble(mean = NA_real_, se_boot = NA_real_, lo = NA_real_, hi = NA_real_, n = length(x)))
  reps <- replicate(R, mean(sample(x, replace = TRUE)))
  tibble(mean = mean(x), se_boot = sd(reps),
         lo = quantile(reps, 0.025, names = FALSE), hi = quantile(reps, 0.975, names = FALSE),
         n = length(x))
}

#' R² of %BF with several indices, and paired differences from a reference index
#'
#' One bootstrap loop: each resample evaluates every index on the same participants, so
#' the R² CIs and the ΔR² CIs (index − reference) come from the same paired resamples.
#' @param d       Data with `pbf` and the index columns.
#' @param indices Named character vector, display name = column, e.g. c(BMI = "bmi").
#' @param ref     Display name of the reference index (here "Benn").
#' @return List of two tibbles: `r2` (index, r, r2, lo, hi, n) and
#'   `diff` (index, delta_r2, lo, hi, n).
r2_boot <- function(d, indices, ref, R = opts$n_boot) {
  d <- d |> select(pbf, all_of(unname(indices))) |> drop_na()
  n <- nrow(d)
  r2_of <- function(i) vapply(indices, function(v) cor(d$pbf[i], d[[v]][i])^2, numeric(1))
  obs  <- r2_of(seq_len(n))
  reps <- replicate(R, r2_of(sample(n, replace = TRUE)))   # matrix: index × R
  others <- setdiff(names(indices), ref)
  list(
    r2 = tibble(index = names(indices), r = sqrt(obs), r2 = obs,
                lo = apply(reps, 1, quantile, 0.025, names = FALSE),
                hi = apply(reps, 1, quantile, 0.975, names = FALSE), n = n),
    diff = tibble(index    = paste(others, "−", ref),
                  delta_r2 = obs[others] - obs[[ref]],
                  lo = apply(reps[others, , drop = FALSE] - rep(reps[ref, ], each = length(others)), 1, quantile, 0.025, names = FALSE),
                  hi = apply(reps[others, , drop = FALSE] - rep(reps[ref, ], each = length(others)), 1, quantile, 0.975, names = FALSE),
                  n = n)
  )
}

#' Cross-sectional growth velocity (cm/year)
#'
#' Difference in median height between consecutive single years of age, within sex,
#' plotted at the older age.
#' @return Tibble: sex, age, median_height_cm, velocity, n.
growth_velocity <- function(df) {
  df |>
    group_by(sex, age) |>
    summarise(median_height_cm = median(height_cm), n = n(), .groups = "drop") |>
    arrange(sex, age) |>
    group_by(sex) |>
    mutate(velocity = median_height_cm - dplyr::lag(median_height_cm)) |>
    ungroup()
}

#' CDC LMS: BMI z-score (Table 1, Eq. 7), BMI at a given z, and the maximum attainable z
lms_z    <- function(bmi, L, M, S) ((bmi / M)^L - 1) / (L * S)
lms_bmi  <- function(z, L, M, S) ifelse(z < lms_zmax(L, S), M * (1 + L * S * z)^(1 / L), NA_real_)
lms_zmax <- function(L, S) -1 / (L * S)     # finite because L < 0 at every age

#' Axis labels with a true minus sign (U+2212) for negative numbers
lab_minus <- scales::label_number(style_negative = "minus", big.mark = "")

#' Midpoint age of an adult age-band label, used as the drift x-position
#'
#' "21–29" -> 25.5, "30–39" -> 35, ..., "70+" -> 75 (see DISCREPANCIES Q6). Unlike v1, a
#' single-year youth label is returned unchanged ("8" -> 8, not 13).
midpts <- function(g) {
  g    <- as.character(g)
  lo   <- as.numeric(str_extract(g, "^[0-9]+"))
  hi   <- as.numeric(str_extract(g, "(?<=[-\u2013])[0-9]+$"))
  case_when(str_detect(g, "\\+$") ~ lo + 5,               # open band: "70+" -> 75
            !is.na(hi)              ~ (lo + hi + 1) / 2,    # "21–29" -> 25.5 (same as v1)
            TRUE                    ~ lo)                   # single year: "8" -> 8
}

#' Drift in %BF at fixed BMI: OLS slope of %BF-at-reference-BMI on stratum age
#'
#' @param sweep  Output of `pbf_at_ref(curves, ref_bmis)` with an `age_x` column (x position).
#' @return Tibble: cohort, sex, ref_bmi, n_strata, pp_per_year, full_support
#'   (`full_support` = all strata of that cohort × sex contribute at this reference BMI).
drift_sweep <- function(sweep) {
  sweep |>
    group_by(cohort, sex, ref_bmi) |>
    summarise(n_strata = n(),
              pp_per_year = coef(lm(pbf ~ age_x))[["age_x"]],
              .groups = "drop") |>
    group_by(cohort, sex) |>
    mutate(full_support = n_strata == max(n_strata)) |>
    ungroup()
}
