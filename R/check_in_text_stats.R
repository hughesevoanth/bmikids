# File Metadata ################################################################
# Title: Compare reproduced in-text numbers with the values quoted in the manuscript
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : stats/manuscript_values.csv                (quoted values; one row per number)
#         <paths$out_stats>/reproduced_*.csv          (written by save_stats() in 01–04)
# Output: <paths$out_stats>/in_text_stats.csv
#         columns: key, manuscript_location, quoted_value, reproduced_value, match, source_script
#
# A value "matches" when the reproduced value, rounded to the number of decimals shown in
# the manuscript, equals the quoted value.

source("R/setup.R")

#' Number of decimal places in a quoted number, e.g. "0.049" -> 3, "18" -> 0
n_decimals <- function(x) {
  frac <- str_extract(x, "(?<=\\.)[0-9]+$")
  if_else(is.na(frac), 0L, nchar(frac))
}

quoted <- read_csv(fs::path(paths$repo, "stats", "manuscript_values.csv"),
                   col_types = cols(.default = "c"))

reproduced <- fs::dir_ls(paths$out_stats, regexp = "reproduced_.*\\.csv$") |>
  map_dfr(read_csv, show_col_types = FALSE, col_types = cols(note = "c"))

# counts come from 01's sample-size table
n_tab <- read_csv(fs::path(paths$out_stats, "sample_sizes.csv"), show_col_types = FALSE)
n_lookup <- c(n_youth       = "Youth (2-20 y)",
              n_youth_dxa_f = "Youth 8-20 y with DXA, females",
              n_youth_dxa_m = "Youth 8-20 y with DXA, males")
reproduced <- bind_rows(
  reproduced,
  tibble(key = names(n_lookup),
         reproduced_value = n_tab$n[match(n_lookup, n_tab$quantity)],
         source_script = "01_data_wrangling")
) |> distinct(key, .keep_all = TRUE)

in_text <- quoted |>
  left_join(reproduced |> select(key, unrounded = reproduced_value, note, script = source_script), by = "key") |>
  mutate(digits        = n_decimals(quoted_value),
         at_quoted_dp  = round(unrounded, digits),                    # precision used for `match`
         match         = at_quoted_dp == as.numeric(quoted_value),
         # what is shown: rounded like the quoted value, but never collapsed to 0
         # (e.g. 0.087 against a quoted "1" is shown as 0.087, not 0)
         shown         = if_else(at_quoted_dp == 0 & unrounded != 0, signif(unrounded, 2), at_quoted_dp),
         source_script = coalesce(script, source_script)) |>
  transmute(key, manuscript_location, quoted_value,
            reproduced_value = shown, reproduced_unrounded = signif(unrounded, 6),
            match, source_script, note)

write_excel_csv(in_text, fs::path(paths$out_stats, "in_text_stats.csv"), na = "")

## the same table as a formatted workbook: mismatches highlighted ##############
wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "in_text_stats")
openxlsx::writeData(wb, "in_text_stats", in_text, headerStyle = openxlsx::createStyle(textDecoration = "bold"))
openxlsx::freezePane(wb, "in_text_stats", firstRow = TRUE)
openxlsx::setColWidths(wb, "in_text_stats", cols = seq_along(in_text), widths = c(26, 55, 14, 16, 20, 8, 20, 40))
mismatch <- which(in_text$match %in% FALSE) + 1                       # +1 for the header row
if (length(mismatch)) {
  openxlsx::addStyle(wb, "in_text_stats", openxlsx::createStyle(fgFill = "#F8D7DA"),
                     rows = mismatch, cols = seq_along(in_text), gridExpand = TRUE)
}
openxlsx::saveWorkbook(wb, fs::path(paths$out_stats, "in_text_stats.xlsx"), overwrite = TRUE)

done <- in_text |> filter(!is.na(match))
cli::cli_alert_info("{nrow(done)} of {nrow(in_text)} values reproduced; {sum(done$match)} match, {sum(!done$match)} differ.")
print(done |> select(key, quoted_value, reproduced_value, match), n = Inf)
