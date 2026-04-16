# ## 00_nhanes_fetch_targets.R
# ## Targeted NHANES fetch: downloads XPT + docs for selected datasets and logs provenance.
# ## by: David Hughes and ChatGPT 5
# ## date: Nov 4 2025 (updated: add DXA pull)

# ------------------------------- Packages ------------------------------------
pkgs <- c("rvest","httr2","xml2","fs","cli","readr","dplyr","stringr","purrr","urltools")
to_install <- setdiff(pkgs, rownames(installed.packages()))
if (length(to_install)) install.packages(to_install, quiet = TRUE)
invisible(lapply(pkgs, library, character.only = TRUE))

# ------------------------------- Config --------------------------------------
OUTDIR <- fs::path_expand("../data/raw/nhanes_data")  # change if desired
fs::dir_create(OUTDIR)

# Only collect these cycles (kept from your newer script)
ALLOWED_CYCLES <- c(
  "1999-2000","2001-2002","2003-2004","2005-2006",
  "2007-2008","2009-2010",
  "2011-2012","2013-2014","2015-2016",
  "2017-2018", ## this is a specific DXA cycle
  "2017-2020","2021-2023"
)

urls <- list(
  index_main  = "https://wwwn.cdc.gov/nchs/nhanes/default.aspx",
  exam_all    = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination",
  demo_all    = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics",
  lab_all     = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory",
  dxa_special = "https://wwwn.cdc.gov/Nchs/Nhanes/Dxa/Dxa.aspx"
)

LOG <- fs::path(OUTDIR, "nhanes_fetch_log.csv")
timestamp_now <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")

# Normalize cycle text like “2017–2018” to “2017-2018”
norm_years <- function(x) x |>
  stringr::str_replace_all("[\u2010-\u2015\u2212]", "-") |>
  stringr::str_squish()

# ------------------------------- Helpers -------------------------------------
req_html <- function(url) {
  resp <- httr2::request(url) |>
    req_user_agent("nhanes-fetch-R/1.0 (contact: you@example.org)") |>
    req_perform()
  if (resp_status(resp) >= 400) rlang::abort(paste("HTTP error:", resp_status(resp), url))
  resp_body_html(resp)
}

clean <- function(x) {
  x |>
    stringr::str_replace_all("[^0-9A-Za-z _.-]", "_") |>
    stringr::str_squish()
}

save_file <- function(url, dest) {
  fs::dir_create(fs::path_dir(dest))
  r <- httr2::request(url) |> req_user_agent("nhanes-fetch-R/1.0")
  resp <- tryCatch(req_perform(r), error = function(e) e)
  if (inherits(resp, "error") || httr2::resp_status(resp) >= 400) {
    return(tibble(status = "error", bytes = NA_real_))
  }
  writeBin(httr2::resp_body_raw(resp), dest)
  tibble(status = "ok", bytes = as.double(fs::file_size(dest)))
}

# Always keep fetched_at as character so we never hit type conflicts when appending
add_log <- function(df, log_path) {
  df$fetched_at <- as.character(timestamp_now())
  if (!fs::file_exists(log_path)) {
    readr::write_csv(df, log_path)
  } else {
    prev <- readr::read_csv(
      log_path,
      show_col_types = FALSE,
      col_types = readr::cols(
        .default   = readr::col_guess(),
        fetched_at = readr::col_character()
      )
    )
    common <- intersect(names(prev), names(df))
    out <- dplyr::bind_rows(
      dplyr::select(prev, dplyr::all_of(common)),
      dplyr::select(df,   dplyr::all_of(common))
    )
    readr::write_csv(out, log_path)
  }
}

# Generic row extractor: match rows by text regex; find Doc/Data links anywhere in the row
extract_rows <- function(page, base_url, contains_regex) {
  empty <- tibble(
    years = character(),
    title = character(),
    doc_url  = character(),
    data_url = character()
  )
  
  all_rows <- page |> html_elements("table tr")
  keep_rows <- purrr::keep(all_rows, ~ {
    txt <- html_text2(.x)
    stringr::str_detect(txt, contains_regex)
  })
  if (length(keep_rows) == 0) return(empty)
  
  purrr::map_dfr(keep_rows, function(tr) {
    tds <- tr |> html_elements("td")
    if (length(tds) < 2) return(NULL)
    
    years <- tds[[1]] |> html_text2() |> norm_years()
    title <- tds[[2]] |> html_text2() |> stringr::str_squish()
    
    links <- tr |> html_elements("a")
    link_text <- links |> html_text2()
    link_href <- links |> html_attr("href")
    
    # Doc link: first anchor whose text contains "Doc"
    doc_idx <- which(stringr::str_detect(link_text, regex("\\bDoc\\b", ignore_case = TRUE)))[1]
    doc_href <- if (!is.na(doc_idx)) link_href[[doc_idx]] else NA_character_
    
    # Data link: first anchor whose text contains "Data [XPT" OR href ends with .xpt
    data_idx1 <- which(stringr::str_detect(link_text, fixed("Data [XPT", ignore_case = TRUE)))[1]
    data_idx2 <- which(stringr::str_detect(link_href, regex("\\.xpt$", ignore_case = TRUE)))[1]
    data_idx <- if (!is.na(data_idx1)) data_idx1 else data_idx2
    data_href <- if (!is.na(data_idx)) link_href[[data_idx]] else NA_character_
    
    tibble(
      years   = years,
      title   = title,
      doc_url  = if (!is.na(doc_href))  url_absolute(doc_href,  base_url) else NA_character_,
      data_url = if (!is.na(data_href)) url_absolute(data_href, base_url) else NA_character_
    )
  })
}

extract_rows_laboratory <- function(page, contains_regex) {
  extract_rows(page, urls$lab_all, contains_regex)
}

# ------------------------------- Load pages ----------------------------------
cli::cli_h1("NHANES DEMO + BMX + DXA targeted fetch (filtered cycles)")
cli::cli_inform(c("i" = "Output dir: {OUTDIR}"))
cli::cli_inform(c("i" = "Log file:   {LOG}"))

exam_page <- req_html(urls$exam_all)
demo_page <- req_html(urls$demo_all)

# Added for DXA (special DXA page for some cycles)
dxa_page  <- req_html(urls$dxa_special)

# ------------------------------- Targets -------------------------------------
# Body Measures (BMX) — exact title match to avoid "Arthritis Body Measures"
re_bmx_seed <- "Body Measures"

# Demographics (DEMO): flexible &/and
re_demo <- "(?i)Demograph\\w*\\s+Variables\\s*(?:&|and)\\s*Sample\\s+Weights"

# DXA: robust to hyphen/en-dash variants
re_dxa <- "Dual[- ]?Energy X[-–]ray Absorptiometry - Whole Body"

# 1) Body Measures (Examination) -> BMX files
bmx_tbl <- extract_rows(exam_page, urls$exam_all, re_bmx_seed) |>
  dplyr::mutate(component = "Examination") |>
  dplyr::filter(stringr::str_squish(.data$title) == "Body Measures")

# 2) Demographics (DEMO)
demo_tbl <- extract_rows(demo_page, urls$demo_all, re_demo) |>
  dplyr::mutate(component = "Demographics")

# 3) DXA Whole Body:
#    3a) From Examination component index (covers later cycles if present there)
exam_dxa_tbl <- extract_rows(exam_page, urls$exam_all, re_dxa) |>
  dplyr::mutate(component = "Examination")

#    3b) From dedicated DXA page (covers early cycles that are not reliably discoverable via component index)
dxa_rows <- dxa_page |>
  html_elements("table tr") |>
  purrr::keep(~ html_text2(.x) |> stringr::str_detect(re_dxa)) |>
  purrr::map_dfr(function(tr) {
    tds <- tr |> html_elements("td")
    if (length(tds) < 2) return(NULL)
    
    years <- tds[[1]] |> html_text2() |> norm_years()
    title <- tds[[2]] |> html_text2() |> stringr::str_squish()
    
    links <- tr |> html_elements("a")
    lt <- links |> html_text2()
    lh <- links |> html_attr("href")
    
    doc_idx  <- which(stringr::str_detect(lt, regex("\\bDoc\\b", ignore_case = TRUE)))[1]
    data_idx1 <- which(stringr::str_detect(lt, fixed("Data [XPT", ignore_case = TRUE)))[1]
    data_idx2 <- which(stringr::str_detect(lh, regex("\\.xpt$", ignore_case = TRUE)))[1]
    data_idx  <- if (!is.na(data_idx1)) data_idx1 else data_idx2
    
    doc_href  <- if (!is.na(doc_idx))  lh[[doc_idx]]  else NA_character_
    data_href <- if (!is.na(data_idx)) lh[[data_idx]] else NA_character_
    
    tibble(
      years     = years,
      title     = title,
      doc_url   = if (!is.na(doc_href))  url_absolute(doc_href,  urls$dxa_special) else NA_character_,
      data_url  = if (!is.na(data_href)) url_absolute(data_href, urls$dxa_special) else NA_character_,
      component = "Examination"
    )
  })

dxa_all <- dplyr::bind_rows(exam_dxa_tbl, dxa_rows) |>
  dplyr::mutate(years = norm_years(.data$years)) |>
  dplyr::distinct(component, years, title, .keep_all = TRUE)

# Combine unique targets, then filter to ALLOWED_CYCLES
targets <- dplyr::bind_rows(bmx_tbl, demo_tbl, dxa_all) |>
  dplyr::distinct(component, years, title, .keep_all = TRUE) |>
  dplyr::mutate(years = norm_years(.data$years)) |>
  dplyr::filter(.data$years %in% ALLOWED_CYCLES)

cli::cli_inform(c("i" = "Found {nrow(targets)} rows across DEMO + BMX + DXA"))
cli::cli_inform(c("i" = "Cycles kept: {toString(sort(unique(targets$years)))}"))
cli::cli_inform(c("i" = "Non-NA URLs: data={sum(!is.na(targets$data_url))}, doc={sum(!is.na(targets$doc_url))}"))
cli::cli_inform(c("i" = "First few data URLs:"))
print(utils::head(
  targets |>
    dplyr::filter(!is.na(.data$data_url)) |>
    dplyr::select(component, years, title, data_url),
  6
))

# ------------------------------- Download ------------------------------------
download_row <- function(years, title, doc_url, data_url, component, ...) {
  comp  <- clean(component)
  yrs   <- clean(years)
  ttl   <- clean(title)
  base  <- fs::path(OUTDIR, comp, ttl, yrs)  # keep your existing foldering
  fs::dir_create(base)
  
  out <- list()
  
  # Data (.XPT)
  if (!is.na(data_url) && nzchar(data_url)) {
    data_name <- basename(urltools::url_parse(data_url)$path)
    if (!nzchar(data_name)) data_name <- "data.xpt"
    data_path <- fs::path(base, data_name)
    res <- save_file(data_url, data_path)
    out[[length(out)+1]] <- tibble(
      component = comp, title = ttl, years = yrs,
      type = "data", source_url = data_url,
      local_path = data_path, status = res$status, bytes = res$bytes
    )
  }
  
  # Doc
  if (!is.na(doc_url) && nzchar(doc_url)) {
    doc_name <- basename(urltools::url_parse(doc_url)$path)
    if (!nzchar(doc_name)) doc_name <- "doc.html"
    doc_path <- fs::path(base, doc_name)
    res <- save_file(doc_url, doc_path)
    out[[length(out)+1]] <- tibble(
      component = comp, title = ttl, years = yrs,
      type = "doc", source_url = doc_url,
      local_path = doc_path, status = res$status, bytes = res$bytes
    )
  }
  
  if (length(out)) {
    df <- dplyr::bind_rows(out)
    add_log(df, LOG)
    df
  } else {
    tibble(
      component = comp, title = ttl, years = yrs,
      type = NA_character_, source_url = NA_character_,
      local_path = NA_character_, status = "no-links", bytes = NA_real_,
      fetched_at = as.character(timestamp_now())
    )
  }
}

cli::cli_inform(c("i" = "Starting downloads..."))
results <- purrr::pmap_dfr(targets, download_row)

cli::cli_alert_success("Done. {sum(results$type=='data', na.rm=TRUE)} data files and {sum(results$type=='doc', na.rm=TRUE)} docs processed.")
cli::cli_inform(c("i" = "Log written to: {LOG}"))
