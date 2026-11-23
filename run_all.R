# File Metadata ################################################################
# Title: Run the whole pipeline, in order, each step in a fresh R session
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Usage (from the repository root):   Rscript run_all.R
#
# Steps: download NHANES (skips files already present) → build datasets → knit the three
# analysis reports → Table 2 → Figures 1–11 → source-data workbook → in-text number check →
# sample-size check (raw recount vs datasets vs every figure and table).
# Each step runs in its own Rscript process, so nothing leaks between scripts.

source("R/setup.R")

rscript <- fs::path(R.home("bin"), "Rscript")

#' Run one step in a fresh R session; stop the pipeline if it fails
#'
#' @param label What the step does (printed).
#' @param expr  R code to run, as a string.
run_step <- function(label, expr) {
  cli::cli_h2(label)
  t0  <- Sys.time()
  log <- tempfile(fileext = ".log")
  status <- system2(rscript, c("-e", shQuote(expr)), stdout = log, stderr = log)
  secs <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")))
  if (status != 0) {
    cat(tail(readLines(log), 30), sep = "\n")
    cli::cli_abort("Step failed: {label} (full log: {.file {log}})")
  }
  cli::cli_alert_success("{label} ({secs} s)")
  tibble(step = label, seconds = secs)
}

render <- function(rmd) sprintf("rmarkdown::render('%s', output_dir = 'reports', quiet = TRUE)", rmd)
script <- function(r)   sprintf("source('%s')", r)

steps <- c(
  "Download NHANES files"           = script("00_nhanes_fetch_data.R"),
  "Build analysis datasets"         = script("01_data_wrangling.R"),
  "Adults (02_adults.Rmd)"          = render("02_adults.Rmd"),
  "Youth (03_kids.Rmd)"             = render("03_kids.Rmd"),
  "Life course (04_kids_and_adults.Rmd)" = render("04_kids_and_adults.Rmd"),
  "Table 2"                         = script("tables/tab02_beta_by_age.R"),
  set_names(map_chr(sprintf("figures/fig%02d_", 1:11), \(p) script(fs::dir_ls("figures", regexp = p))),
            paste("Figure", 1:11)),
  "Graphical abstract"              = script("figures/graphical_abstract.R"),
  "Source-data workbook"            = script("figures/make_source_data_workbook.R"),
  "In-text number check"            = script("R/check_in_text_stats.R"),
  "Sample-size check"               = script("R/check_sample_sizes.R")
)

cli::cli_h1("BMI in kids: full pipeline")
cli::cli_inform(c("i" = "Data:    {.file {paths$data}}", "i" = "Outputs: {.file {paths$out}}"))

timings <- imap_dfr(steps, \(expr, label) run_step(label, expr))

## record the run ##############################################################
writeLines(c(paste("Pipeline completed:", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")), "",
             capture.output(print(timings, n = Inf)), "",
             capture.output(sessioninfo::session_info())),
           fs::path(paths$out_stats, "session_info.txt"))

cli::cli_h1("Done in {round(sum(timings$seconds) / 60, 1)} min")
cli::cli_inform(c("i" = "Figures:      {.file {paths$out_figures}}",
                  "i" = "Source data:  {.file {paths$out_source}}",
                  "i" = "Tables:       {.file {paths$out_tables}}",
                  "i" = "In-text stats {.file {fs::path(paths$out_stats, 'in_text_stats.csv')}}",
                  "i" = "Reports:      {.file {fs::path(paths$repo, 'reports')}}"))
