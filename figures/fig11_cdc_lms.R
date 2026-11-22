# File Metadata ################################################################
# Title: Figure 11 — CDC 2022 BMI-for-age LMS parameters (A–C) and percentile curves (D)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig11.rds (03_kids.Rmd; from data/bmi-age-2022.csv, CDC)
# Output: results/source_data/SourceData_Fig11.csv; results/figures/Figure11.pdf / .png
#
# Ages 2–20 years (CDC table rows 24–240 months), plotted in years. Standard sex colours.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

res <- read_result("res_fig11")

## source data #################################################################
src <- bind_rows(
  res$lms |> pivot_longer(c(L, M, S), names_to = "parameter", values_to = "y") |>
    transmute(panel = recode(parameter, L = "A", M = "B", S = "C"), parameter, sex = as.character(sex),
              agemos, x = age, y),
  res$percentiles |> transmute(panel = "D", parameter = "BMI percentile", sex = as.character(sex),
                               percentile = as.character(percentile), agemos, x = age, y = bmi)
) |>
  mutate(element = "line_vertex")
pct_levels <- levels(res$percentiles$percentile)
src <- write_source_data(src, 11) |>
  mutate(percentile = factor(percentile, levels = pct_levels))

## Figure 11A–C: L, M, S #######################################################
lms_panel <- function(id, ylab, x_title = NULL) {
  ggplot(filter(src, panel == id), aes(x, y, colour = sex)) +
    geom_line(linewidth = 1.3) +
    scale_colour_manual(values = pal_sex, limits = c("Females", "Males"), name = "Sex") +
    scale_x_continuous(breaks = seq(2, 20, 2)) +
    scale_y_continuous(labels = lab_minus) +
    labs(x = x_title, y = ylab) +
    theme_bmikids() +
    theme(panel.grid.minor.x = element_blank())
}
pA <- lms_panel("A", "L (Box-Cox power)")
pB <- lms_panel("B", "M (median BMI, kg/m²)")
pC <- lms_panel("C", "S (coefficient of variation)", "Age (years)")

## Figure 11D: percentile curves ###############################################
pD <- ggplot(filter(src, panel == "D"), aes(x, y, colour = percentile)) +
  geom_line(linewidth = 1.1) +
  facet_wrap(~ sex) +
  scale_colour_manual(values = set_names(pal_percentile(length(pct_levels)), pct_levels), name = "Percentile") +
  scale_x_continuous(breaks = seq(2, 20, 2)) +
  scale_y_continuous(breaks = seq(10, 40, 5)) +
  labs(x = "Age (years)", y = "BMI (kg/m²)") +
  theme_bmikids() +
  theme(panel.grid.minor.x = element_blank())

fig11 <- ((pA / pB / pC) | pD) +
  plot_layout(widths = c(1, 2), guides = "collect") +
  plot_annotation(tag_levels = "A")

save_figure(fig11, "Figure11", width = 13, height = 8)

sessioninfo::session_info()
