# File Metadata ################################################################
# Title: Figure 3 — β by age (A), growth velocity (B), BMI–height correlation (C), ages 2–20
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig3.rds (03_kids.Rmd)
# Output: results/source_data/SourceData_Fig3.csv; results/figures/Figure3.pdf / .png
#
# All curves are thin-plate GAM smooths (bs = "tp") of the plotted estimates.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

res <- read_result("res_fig3")
dodge <- c(All = 0, Females = -0.15, Males = 0.15)       # horizontal offset of points by sex

## source data #################################################################
est <- bind_rows(
  res$beta |> transmute(panel = "A", sex, age, x = age, y = beta, lo, hi, n, p = NA_real_),
  res$velocity |> transmute(panel = "B", sex, age, x = age, y = velocity, lo = NA, hi = NA, n, p = NA_real_),
  res$r_bmi_height |> transmute(panel = "C", sex, age, x = age + dodge[as.character(sex)],
                                y = r, lo, hi, n, p)
) |>
  mutate(sex = as.character(sex), element = "estimate",
         star = !is.na(p) & sex != "All" & p < opts$p_star)

smooths <- est |>
  smooth_by("age", "y", group = c("panel", "sex")) |>
  mutate(sex = as.character(sex))

src <- bind_rows(est, smooths) |>
  mutate(y_variable = recode(panel, A = "Allometric exponent beta", B = "Growth velocity (cm/year)",
                             C = "Pearson r, height vs BMI"), .after = panel)
src <- write_source_data(src, 3)

## panels ######################################################################
sex_scale <- list(scale_colour_manual(values = pal_sex, limits = c("All", "Females", "Males"), name = "Sex"),
                  scale_x_continuous(breaks = 2:20, limits = c(1.6, 20.4)),
                  theme_bmikids(),
                  theme(panel.grid.minor.x = element_blank()))
pts <- function(id) filter(src, panel == .env$id, element == "estimate")
crv <- function(id) filter(src, panel == .env$id, element == "smooth")

### Figure 3A: β by age (points: sex-adjusted; curves: smooths of F, M and adjusted)
pA <- ggplot() +
  geom_hline(yintercept = 2, linetype = "dashed", linewidth = 0.8) +
  geom_line(data = crv("A"), aes(x, y, colour = sex), linewidth = 1.1) +
  geom_errorbar(data = pts("A") |> filter(sex == "All"), aes(x, ymin = lo, ymax = hi), width = 0.2, linewidth = 0.7) +
  geom_point(data = pts("A") |> filter(sex == "All"), aes(x, y, colour = sex), size = 3) +
  labs(x = NULL, y = "Allometric exponent β") +
  sex_scale

### Figure 3B: growth velocity
pB <- ggplot() +
  geom_line(data = crv("B"), aes(x, y, colour = sex), linewidth = 1.1) +
  geom_point(data = pts("B"), aes(x, y, colour = sex), size = 3) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = NULL, y = "Growth velocity (cm/year)") +
  sex_scale +
  guides(colour = "none")                                   # one shared legend, from panel A

### Figure 3C: BMI–height correlation, with asterisks for p < 0.05
pC <- ggplot() +
  geom_hline(yintercept = 0, linetype = "dashed", linewidth = 0.8) +
  geom_line(data = crv("C"), aes(x, y, colour = sex), linewidth = 1.1) +
  geom_errorbar(data = pts("C") |> filter(sex != "All"), aes(x, ymin = lo, ymax = hi, colour = sex),
                width = 0.15, linewidth = 0.7) +
  geom_point(data = pts("C") |> filter(sex != "All"), aes(x, y, colour = sex), size = 3) +
  geom_point(data = pts("C") |> filter(star), aes(x, hi + 0.025, colour = sex),
             shape = 8, size = 1.8, show.legend = FALSE) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = "Age (years)", y = "Pearson's r (height, BMI)") +
  sex_scale +
  guides(colour = "none")

fig3 <- (pA / pB / pC) +
  plot_layout(guides = "collect") +
  plot_annotation(tag_levels = "A")

save_figure(fig3, "Figure3", width = 6.5, height = 12)

sessioninfo::session_info()
