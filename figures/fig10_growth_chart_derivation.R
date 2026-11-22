# File Metadata ################################################################
# Title: Figure 10 — steps in deriving BMI-for-age percentiles (illustration; males 2–19 y)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig10.rds (03_kids.Rmd); data/derived/kids.rds (panel A points only)
# Output: results/source_data/SourceData_Fig10.csv; results/figures/Figure10.pdf / .png
#
# (A) individual BMI by age; (B) unweighted empirical percentiles in single-year age groups;
# (C) percentiles smoothed across age with GAMs (k = 8). This illustrates the CDC procedure;
# it does not reproduce the CDC charts. All panels share y-limits 10–43, breaks every 10.
# Source data for panel A are counts in 1-year × 1 kg/m² cells (no individual rows).

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

res   <- read_result("res_fig10")
males <- read_result("kids") |> filter(sex == "Males", age >= 2, age <= 19)
y_lim <- c(10, res$y_cap)

## source data #################################################################
bins <- males |>
  transmute(x = age, y = floor(bmi) + 0.5) |>          # 1-year × 1 kg/m² cells
  count(x, y, name = "n") |>
  mutate(panel = "A", element = "bin_count")

pct <- res$percentiles |>
  transmute(panel = "B", element = "empirical_percentile", percentile = as.character(percentile),
            x = age, y = bmi, n)

smooths <- pct |>
  smooth_by("x", "y", group = "percentile", k = 8) |>
  mutate(panel = "C")

src <- bind_rows(bins, pct, pct |> mutate(panel = "C"), smooths) |>
  relocate(panel, element, percentile)
src <- write_source_data(src, 10) |>
  mutate(percentile = factor(percentile, levels = levels(res$percentiles$percentile)))

pal10 <- set_names(pal_percentile(10), levels(src$percentile))
cli::cli_alert_info("{sum(males$bmi > res$y_cap)} of {nrow(males)} boys above {res$y_cap} kg/m² are outside the plotted range")

## panels ######################################################################
shared <- list(scale_y_continuous(breaks = seq(10, 40, 10)),
               labs(x = "Age (years)", y = "BMI (kg/m²)"),
               theme_bmikids(base_size = 12),
               theme(legend.position = "none"))

pA <- ggplot(males, aes(age, bmi)) +
  geom_point(size = 0.3, alpha = 0.15, colour = "grey40") +
  coord_cartesian(ylim = y_lim) + shared

pB <- ggplot(filter(src, panel == "B"), aes(x, y, colour = percentile)) +
  geom_point(size = 0.9) +
  scale_colour_manual(values = pal10) +
  coord_cartesian(ylim = y_lim) + shared

lab_end <- filter(src, panel == "C", element == "smooth") |> slice_max(x, n = 1, by = percentile)
pC <- ggplot(mapping = aes(x, y, colour = percentile)) +
  geom_point(data = filter(src, panel == "C", element == "empirical_percentile"), size = 0.7) +
  geom_line(data = filter(src, panel == "C", element == "smooth"), linewidth = 0.6) +
  ggrepel::geom_text_repel(data = lab_end, aes(label = percentile), direction = "y", hjust = 0,
                           nudge_x = 0.8, xlim = c(max(lab_end$x) + 0.4, NA), size = 3,
                           fontface = "bold", segment.size = 0.2, min.segment.length = 0, seed = 1) +
  scale_colour_manual(values = pal10) +
  scale_x_continuous(expand = expansion(mult = c(0.02, 0.15))) +
  coord_cartesian(ylim = y_lim, clip = "off") + shared

fig10 <- (pA + pB + pC) + plot_annotation(tag_levels = "A")
save_figure(fig10, "Figure10", width = 10, height = 3.2)

sessioninfo::session_info()
