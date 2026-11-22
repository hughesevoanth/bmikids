# File Metadata ################################################################
# Title: Figure 2 — the three BMI assumptions across the life course (sex-adjusted)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig2.rds (04_kids_and_adults.Rmd)
# Output: results/source_data/SourceData_Fig2.csv; results/figures/Figure2.pdf / .png
#
# (A) β; (B) partial r of height with %BF; (C) %BF at BMI 25. Points at the median age of each
# stratum, sized by n on one shared scale; youth ages stretched twofold; divider at 21 y.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

res <- read_result("res_fig2")

## source data #################################################################
src <- res$estimates |>
  transmute(panel, cohort, stratum, n, x = age_mid, y = est, lo, hi,
            y_variable = recode(panel, A = "Allometric exponent beta (sex-adjusted)",
                                B = "Partial r, height vs %BF (sex-adjusted)",
                                C = "%BF at BMI 25 (sex-adjusted GAM)"),
            element = "estimate") |>
  bind_rows(tibble(panel = "C", element = "reference_line", y = res$adult_mean_pbf25,
                   y_variable = "Mean %BF at BMI 25 across adult strata"))
src <- write_source_data(src, 2)

## shared scales: identical in every panel so the legends merge ################
x_limits <- range(src$x, na.rm = TRUE)
n_limits <- range(src$n, na.rm = TRUE)
shared <- list(
  geom_vline(xintercept = 21, linetype = "dashed"),
  scale_x_lifecourse(limits = x_limits),
  scale_colour_manual(values = pal_cohort[lbl_cohort[c("adults", "youth")]], name = NULL),
  scale_size_continuous(name = "n", range = c(1, 4), limits = n_limits, breaks = c(1000, 2500, 5000, 7500)),
  guides(colour = guide_legend(order = 1), size = guide_legend(order = 2)),
  theme_bmikids(),
  theme(axis.title.y = element_markdown())
)
pts <- function(id) filter(src, panel == .env$id, element == "estimate")
whiskers_and_points <- function(d) list(
  geom_errorbar(data = d, aes(x, ymin = lo, ymax = hi, colour = cohort), width = 0.5),
  geom_point(data = d, aes(x, y, colour = cohort, size = n))
)

### Figure 2A: stable scaling
pA <- ggplot() +
  geom_hline(yintercept = c(2, 3), linetype = "dashed") +
  annotate("text", x = Inf, y = c(2, 3), label = c("BMI (H²)  ", "TMI (H³)  "),
           hjust = 1, vjust = 1.4, size = 2.6, fontface = "bold") +
  whiskers_and_points(pts("A")) +
  labs(x = NULL, y = "<b>Allometric exponent β<br>(W = αH<sup>β</sup>)</b>") +
  shared

### Figure 2B: height–fat independence
pB <- ggplot() +
  geom_hline(yintercept = 0, linetype = "dashed") +
  whiskers_and_points(pts("B")) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = NULL, y = "<b>Partial correlation of<br>height with percent body fat (r)</b>") +
  shared

### Figure 2C: compositional stability
pC <- ggplot() +
  geom_hline(yintercept = res$adult_mean_pbf25, linetype = "dashed") +
  whiskers_and_points(pts("C")) +
  labs(x = "Age (years)", y = "<b>Percent body fat<br>at BMI 25 (%)</b>") +
  shared

fig2 <- (pA / pB / pC) +
  plot_layout(guides = "collect") +
  plot_annotation(tag_levels = "A")

save_figure(fig2, "Figure2", width = 8, height = 8)

sessioninfo::session_info()
