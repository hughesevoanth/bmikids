# File Metadata ################################################################
# Title: Table 2 — allometric exponent β (± SE) by age and sex
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_table2.rds (03_kids.Rmd); stats/manuscript_table2.csv
# Output: results/tables/Table2.csv               full precision (β, SE, 95% CI, n, R²)
#         results/tables/Table2.docx              formatted as in the manuscript
#         results/tables/Table2_vs_manuscript.csv every cell, quoted vs reproduced

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")
library(flextable)

table2 <- read_result("res_table2")

## full-precision CSV ##########################################################
write_excel_csv(table2, fs::path(paths$out_tables, "Table2.csv"))

## formatted table (two decimals everywhere, so 3.00 never prints as "3") ######
tab_fmt <- table2 |>
  mutate(cell = sprintf("%.2f±%.2f", beta, se)) |>
  select(Age = age, sex, cell) |>
  pivot_wider(names_from = sex, values_from = cell) |>
  mutate(Age = as.character(Age))

n_total <- table2 |> filter(sex == "Sex-adjusted") |> pull(n) |> sum()

ft <- flextable(tab_fmt) |>
  set_caption(sprintf("Table 2: Estimates of the allometric height exponent (β±SE) for the weight-stature index W/H^β derived from %s children in NHANES.",
                      format(n_total, big.mark = ","))) |>
  add_footer_lines("Estimates from log–log regression of body weight on height (Equation 3; Table 1), fit separately at each age and by sex. Sex-adjusted estimates include sex as a covariate.") |>
  bold(part = "header") |>
  autofit()
save_as_docx(ft, path = fs::path(paths$out_tables, "Table2.docx"))

## compare with the manuscript, cell by cell ###################################
quoted <- read_csv(fs::path(paths$repo, "stats", "manuscript_table2.csv"), show_col_types = FALSE) |>
  pivot_longer(-age, names_to = c("sex", ".value"), names_sep = "_") |>
  mutate(sex = recode(sex, females = "Females", males = "Males", adjusted = "Sex-adjusted"))

comparison <- table2 |>
  mutate(sex = as.character(sex)) |>
  select(age, sex, n, beta_reproduced = beta, se_reproduced = se) |>
  inner_join(quoted |> rename(beta_quoted = beta, se_quoted = se), by = join_by(age, sex)) |>
  mutate(match_beta = round(beta_reproduced, 2) == beta_quoted,
         match_se   = round(se_reproduced, 2)   == se_quoted)

write_excel_csv(comparison, fs::path(paths$out_tables, "Table2_vs_manuscript.csv"))
cli::cli_alert_info("Table 2: {sum(comparison$match_beta & comparison$match_se)} of {nrow(comparison)} cells match the manuscript.")

sessioninfo::session_info()
