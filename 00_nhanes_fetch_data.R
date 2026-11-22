# File Metadata ################################################################
# Title: Download the NHANES public-use files and the CDC BMI-for-age table used in the review
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Downloads the demographic (DEMO), body measures (BMX) and whole-body DXA (DXX)
# SAS transport files listed in `nhanes_manifest` (R/setup.R) from the CDC website.
#
# - Files already present anywhere under `paths$raw` (matched by file name, any case)
#   are skipped, so re-running is cheap and never overwrites existing data.
# - New files go to <paths$raw>/nhanes_xpt/<cycle>/<FILE>.xpt.
# - Every attempt is logged to <paths$raw>/nhanes_xpt/fetch_log.csv with the URL,
#   size and MD5 checksum.
#
# Usage (from the repository root):  Rscript 00_nhanes_fetch_data.R

source("R/setup.R")
library(httr2)

cli::cli_h1("NHANES download")

## where files go ##############################################################
dest_root <- fs::path(paths$raw, "nhanes_xpt")       # created only if something is downloaded
log_file  <- fs::path(dest_root, "fetch_log.csv")

## what is already on disk #####################################################
fs::dir_create(paths$raw)                            # first run on a fresh clone
existing <-fs::dir_ls(paths$raw, recurse = TRUE, regexp = "(?i)\\.xpt$")
have     <- toupper(fs::path_ext_remove(fs::path_file(existing)))

todo <- nhanes_manifest |> filter(!toupper(file) %in% have)
cli::cli_inform(c("i" = "{nrow(nhanes_manifest)} files in manifest; {nrow(todo)} to download."))

## download one file ###########################################################
#' Download one NHANES file and return a log row
#'
#' @param cycle,file,url From the manifest.
#' @return A one-row tibble describing the attempt.
fetch_one <- function(cycle, file, url, ...) {
  dest <- fs::path(dest_root, cycle, paste0(file, ".xpt"))
  fs::dir_create(fs::path_dir(dest))
  resp <- tryCatch(
    request(url) |>
      req_user_agent("bmikids-reproducibility/2.0 (R httr2)") |>
      req_retry(max_tries = 3) |>
      req_perform(path = dest),
    error = function(e) e
  )
  ok <- !inherits(resp, "error") && fs::file_exists(dest) && fs::file_size(dest) > 0
  if (!ok && fs::file_exists(dest)) fs::file_delete(dest)   # never leave partial files
  if (ok) cli::cli_alert_success("{cycle} {file}") else cli::cli_alert_danger("{cycle} {file}: failed")
  tibble(cycle, file, url,
         local_path = if (ok) as.character(dest) else NA_character_,
         status     = if (ok) "ok" else "error",
         bytes      = if (ok) as.numeric(fs::file_size(dest)) else NA_real_,
         md5        = if (ok) unname(tools::md5sum(dest)) else NA_character_,
         fetched_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC"))
}

## run #########################################################################
if (nrow(todo) > 0) {
  log <- pmap_dfr(todo, fetch_one)
  if (fs::file_exists(log_file)) log <- bind_rows(read_csv(log_file, show_col_types = FALSE,
                                                         col_types = cols(.default = "c")),
                                                  mutate(log, across(everything(), as.character)))
  write_csv(log, log_file)
  n_bad <- sum(log$status != "ok")
  if (n_bad > 0) cli::cli_warn("{n_bad} file(s) failed; re-run to retry.")
}

## final check #################################################################
have_now <- toupper(fs::path_ext_remove(fs::path_file(
  fs::dir_ls(paths$raw, recurse = TRUE, regexp = "(?i)\\.xpt$"))))
missing  <- setdiff(toupper(nhanes_manifest$file), have_now)
if (length(missing)) {
  cli::cli_abort("Still missing: {missing}")
} else {
  cli::cli_alert_success("All {nrow(nhanes_manifest)} NHANES files are available under {.file {paths$raw}}")
}

## CDC 2022 BMI-for-age LMS table (Figure 11, z-score example) #################
cdc_url <- "https://www.cdc.gov/growthcharts/data/extended-bmi/bmi-age-2022.csv"
if (!fs::file_exists(paths$cdc_lms)) {
  fs::dir_create(fs::path_dir(paths$cdc_lms))
  request(cdc_url) |> req_retry(max_tries = 3) |> req_perform(path = paths$cdc_lms)
  cli::cli_alert_success("Downloaded CDC BMI-for-age table to {.file {paths$cdc_lms}}")
} else {
  cli::cli_alert_info("CDC BMI-for-age table already present: {.file {paths$cdc_lms}}")
}

## session info ################################################################
sessioninfo::session_info()
