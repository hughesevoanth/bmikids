# File Metadata ################################################################
# Title: Figure 6 — variance in %BF explained by BMI, TMI and Benn's index (ages 8–20, DXA)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig6.rds (03_kids.Rmd)
# Output: results/source_data/SourceData_Fig6.csv; results/figures/Figure6.pdf / .png
#
# (A) R² by age and sex; (B) ΔR² vs Benn's index; (C) ΔR² against the height–%BF r (Fig 5A),
# with a linear fit and its 95% CI across all age × sex estimates.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

res   <- read_result("res_fig6")
dodge <- c(Females = -0.15, Males = 0.15)

## linear fit for panel C (one per index, across all age × sex points) #########
fit_c <- res$dependence |>
  group_by(index) |>
  group_modify(function(d, key) {
    m  <- lm(delta_r2 ~ r_height_pbf, data = d)
    g  <- tibble(r_height_pbf = seq(min(d$r_height_pbf), max(d$r_height_pbf), length.out = 100))
    pr <- predict(m, newdata = g, interval = "confidence")
    tibble(x = g$r_height_pbf, y = pr[, "fit"], lo = pr[, "lwr"], hi = pr[, "upr"])
  }) |>
  ungroup() |>
  mutate(panel = "C", element = "linear_fit", sex = "All")

## source data #################################################################
est <- bind_rows(
  res$r2   |> transmute(panel = "A", facet = as.character(index), sex = as.character(sex), age,
                        x = age + dodge[sex], y = r2, lo, hi, n),
  res$diff |> transmute(panel = "B", facet = index, sex = as.character(sex), age,
                        x = age + dodge[sex], y = delta_r2, lo, hi, n),
  res$dependence |> transmute(panel = "C", facet = index, sex = as.character(sex), age,
                              x = r_height_pbf, y = delta_r2, lo, hi, n)
) |>
  mutate(element = "estimate")

smooths <- est |>
  filter(panel %in% c("A", "B")) |>
  smooth_by("age", "y", group = c("panel", "facet", "sex"))

src <- bind_rows(est, smooths, fit_c |> rename(facet = index)) |>
  mutate(x_variable = if_else(panel == "C", "Pearson r, height vs %BF (Fig 5A)", "Age (years)"),
         y_variable = if_else(panel == "A", "R-squared, %BF vs index", "Difference in R-squared"),
         .after = panel)
src <- write_source_data(src, 6)

pts <- function(id) filter(src, panel == .env$id, element == "estimate")
crv <- function(id) filter(src, panel == .env$id, element == "smooth")
facet_levels <- list(A = c("BMI", "TMI", "Benn"), B = c("BMI − Benn", "TMI − Benn"))

common <- list(scale_colour_manual(values = pal_sex, limits = c("Females", "Males"), name = "Sex"),
               theme_bmikids())

## Figure 6A: R² ###############################################################
pA <- ggplot(mapping = aes(x, y, colour = sex)) +
  geom_line(data = crv("A"), linewidth = 1.1) +
  geom_errorbar(data = pts("A"), aes(ymin = lo, ymax = hi), width = 0.25, linewidth = 0.6) +
  geom_point(data = pts("A"), size = 2.6) +
  facet_wrap(~ factor(facet, levels = facet_levels$A), nrow = 1) +
  scale_x_continuous(breaks = seq(8, 20, 2)) +
  labs(x = "Age (years)", y = "R² between %BF and the index") +
  common

## Figure 6B: ΔR² vs Benn ######################################################
pB <- ggplot(mapping = aes(x, y, colour = sex)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_vline(xintercept = 11.5, linetype = "dashed") +
  geom_line(data = crv("B"), linewidth = 1.1) +
  geom_errorbar(data = pts("B"), aes(ymin = lo, ymax = hi), width = 0.25, linewidth = 0.6) +
  geom_point(data = pts("B"), size = 2.6) +
  facet_wrap(~ factor(facet, levels = facet_levels$B), nrow = 1) +
  scale_x_continuous(breaks = 8:20) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = "Age (years)", y = "Difference in R²") +
  common + theme(panel.grid.minor.x = element_blank()) +
  guides(colour = "none")                                   # one shared legend, from panel A

## Figure 6C: ΔR² against the height–%BF correlation ###########################
lin <- filter(src, panel == "C", element == "linear_fit")
pC <- ggplot(mapping = aes(x, y)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_vline(xintercept = 0, linetype = "dashed") +
  geom_ribbon(data = lin, aes(ymin = lo, ymax = hi), fill = "grey85") +
  geom_line(data = lin, linewidth = 1) +
  geom_errorbar(data = pts("C"), aes(ymin = lo, ymax = hi, colour = sex), width = 0.004, alpha = 0.7) +
  geom_point(data = pts("C"), aes(colour = sex), size = 2.6) +
  ggrepel::geom_text_repel(data = pts("C"), aes(label = age, colour = sex), size = 3, fontface = "bold",
                           point.size = 2.6, min.segment.length = 0.2, segment.colour = "grey60",
                           max.overlaps = Inf, seed = 1, show.legend = FALSE) +
  facet_wrap(~ factor(facet, levels = facet_levels$B), nrow = 1) +
  scale_x_continuous(labels = lab_minus) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = "Pearson's r between height and %BF", y = "Difference in R²") +
  common +
  guides(colour = "none")

fig6 <- (pA / pB / pC) +
  plot_layout(guides = "collect") +
  plot_annotation(tag_levels = "A")

save_figure(fig6, "Figure6", width = 11, height = 12)

sessioninfo::session_info()
