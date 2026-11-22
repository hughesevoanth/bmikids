# File Metadata ################################################################
# Title: Figure 8 — compositional stability: %BF vs BMI by age stratum (A adults, B youth)
#        and drift in %BF at fixed BMI across reference BMIs (C)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_adult_curves.rds (02), res_youth_curves.rds (03), res_fig8c.rds (04)
# Output: results/source_data/SourceData_Fig8.csv; results/figures/Figure8.pdf / .png
#
# A–B: sex-specific GAM curves within each age stratum, drawn between the stratum's 5th and
# 95th BMI percentiles; ribbons ±1 residual SD; points and labels give %BF at BMI 25.
# C: OLS slope of %BF-at-reference-BMI on stratum age; solid where every stratum contributes.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

adult <- read_result("res_adult_curves")
youth <- read_result("res_youth_curves")
drift <- read_result("res_fig8c")$drift

## source data #################################################################
curve_rows <- function(res, stratum, panel) {
  bind_rows(
    res$curves |> filter(type == "curve") |>
      transmute(panel, element = "curve", sex = as.character(sex), stratum = as.character(.data[[stratum]]),
                n, x = bmi, y = fit, lo = fit - sd_resid, hi = fit + sd_resid),
    res$pbf25 |>
      transmute(panel, element = "pbf_at_bmi25", sex = as.character(sex), stratum = as.character(.data[[stratum]]),
                n, x = ref_bmi, y = pbf, lo, hi)
  )
}
src <- bind_rows(
  curve_rows(adult, "age_decade", "A"),
  curve_rows(youth, "age_grp",    "B"),
  drift |> transmute(panel = "C", element = "drift", cohort, sex = as.character(sex),
                     n = n_strata, x = ref_bmi, y = pp_per_year, full_support)
) |>
  mutate(note = case_when(element == "curve"        ~ "lo/hi = fit -/+ 1 residual SD",
                          element == "pbf_at_bmi25" ~ "lo/hi = 95% CI of the conditional mean",
                          element == "drift"        ~ "y = pp per year of age; n = age strata contributing"))
src <- write_source_data(src, 8)

## Figure 8A/B: curves by stratum ##############################################
curve_panel <- function(id, strata_levels) {
  cv  <- filter(src, panel == id, element == "curve") |> mutate(stratum = factor(stratum, strata_levels))
  ref <- filter(src, panel == id, element == "pbf_at_bmi25") |> mutate(stratum = factor(stratum, strata_levels))

  ## text block of %BF at BMI 25, bottom-right of each facet
  y_rng <- range(c(cv$lo, cv$hi))
  step  <- 0.058 * diff(y_rng)
  x_lab <- max(cv$x) + 0.04 * diff(range(cv$x))
  lab <- ref |>
    arrange(sex, stratum) |>
    group_by(sex) |>
    mutate(y_lab = y_rng[1] + step * (n() - row_number()), txt = sprintf("%s: %.1f", stratum, y)) |>
    ungroup()
  hdr <- lab |> group_by(sex) |> summarise(y_lab = max(y_lab) + step, .groups = "drop") |>
    mutate(txt = paste0("%BF at BMI ", opts$ref_bmi))

  ggplot(cv, aes(x, y, colour = stratum, fill = stratum)) +
    geom_ribbon(aes(ymin = lo, ymax = hi), alpha = 0.10, colour = NA) +
    geom_vline(xintercept = opts$ref_bmi, linetype = "dotted", colour = "grey20") +
    geom_line(linewidth = 0.9) +
    geom_point(data = ref, size = 2.2, show.legend = FALSE) +
    geom_text(data = lab, aes(x = x_lab, y = y_lab, label = txt), hjust = 1, size = 3.2,
              fontface = "bold", show.legend = FALSE) +
    geom_text(data = hdr, aes(x = x_lab, y = y_lab, label = txt), hjust = 1, size = 3.2,
              fontface = "bold", colour = "black", inherit.aes = FALSE) +
    facet_wrap(~ sex) +
    scale_colour_viridis_d(name = "Age (y)", end = 0.9) +
    scale_fill_viridis_d(guide = "none", end = 0.9) +
    guides(colour = guide_legend(override.aes = list(linewidth = 1.8))) +
    labs(x = "BMI (kg/m²)", y = "Percent body fat (DXA)") +
    theme_bmikids(base_size = 13) +
    theme(legend.key.height = unit(0.8, "lines"))
}

pA <- curve_panel("A", levels(adult$curves$age_decade)) + labs(x = NULL)
pB <- curve_panel("B", levels(youth$curves$age_grp))

## Figure 8C: drift across reference BMIs ######################################
dr <- filter(src, panel == "C") |>
  mutate(cohort = factor(cohort, levels = lbl_cohort[c("adults", "youth_dxa")]))

pC <- ggplot(dr, aes(x, y)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_vline(xintercept = opts$ref_bmi, linetype = "dashed") +
  geom_line(linetype = "dotted", colour = "grey20") +                       # full range
  geom_line(data = \(d) filter(d, full_support), colour = "grey20") +      # all strata contribute
  geom_point(aes(size = n, fill = cohort), shape = 21) +
  facet_grid(. ~ cohort + sex) +
  scale_fill_manual(values = pal_cohort, guide = "none") +
  guides(size = guide_legend(override.aes = list(fill = "grey50"))) +   # filled legend keys
  scale_size_continuous(name = "Age strata\ncontributing", range = c(1, 3), breaks = c(6, 8, 10, 12)) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = "Reference BMI (kg/m²)", y = "Drift in %BF per year of age") +
  theme_bmikids(base_size = 13)

fig8 <- (pA / pB / pC) +
  plot_layout(heights = c(2, 2, 1)) +
  plot_annotation(tag_levels = "A")

save_figure(fig8, "Figure8", width = 12, height = 13)

sessioninfo::session_info()
