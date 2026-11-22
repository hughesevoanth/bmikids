# File Metadata ################################################################
# Title: Figure 7 — height, weight (A) and BMI, TMI, Benn's index (B) by age, ages 2–20
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig7.rds (03_kids.Rmd; thin-plate GAM smooths by sex)
# Output: results/source_data/SourceData_Fig7.csv; results/figures/Figure7.pdf / .png

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

src <- read_result("res_fig7") |>
  transmute(panel, measure = as.character(measure), sex = as.character(sex), x = age, y = value, element)
measure_levels <- unique(src$measure)
src <- write_source_data(src, 7) |>
  mutate(measure = factor(measure, levels = measure_levels))

growth_panel <- function(id, ...) {
  ggplot(filter(src, panel == id), aes(x, y, colour = sex)) +
    geom_line(linewidth = 1.6) +
    facet_wrap(~ measure, nrow = 1, scales = "free_y") +
    scale_colour_manual(values = pal_sex, limits = c("Females", "Males"), name = "Sex") +
    scale_x_continuous(breaks = seq(5, 20, 5)) +
    labs(y = NULL, ...) +
    theme_bmikids(base_size = 13) +
    theme(panel.grid.minor = element_blank())
}

fig7 <- (growth_panel("A", x = NULL) / growth_panel("B", x = "Age (years)")) +
  plot_layout(guides = "collect") +
  plot_annotation(tag_levels = "A")

save_figure(fig7, "Figure7", width = 12, height = 8)

sessioninfo::session_info()
