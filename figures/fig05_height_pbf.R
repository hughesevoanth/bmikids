# File Metadata ################################################################
# Title: Figure 5 — height and %BF, ages 8–20 (DXA): r by age and sex (A); %BF by height quartile (B)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig5.rds (03_kids.Rmd)
# Output: results/source_data/SourceData_Fig5.csv; results/figures/Figure5.pdf / .png

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

res   <- read_result("res_fig5")
dodge <- c(Females = -0.12, Males = 0.12)
q_off <- c(`Q1 (shortest)` = -0.21, Q2 = -0.07, Q3 = 0.07, `Q4 (tallest)` = 0.21)

## source data #################################################################
est <- bind_rows(
  res$r_height_pbf |> transmute(panel = "A", sex = as.character(sex), group = as.character(sex), age,
                                x = age + dodge[sex], y = r, lo, hi, n, p),
  res$quartiles    |> transmute(panel = "B", sex = as.character(sex), group = as.character(height_q), age,
                                x = age + q_off[as.character(height_q)], y = mean, lo, hi, n, p = NA_real_)
) |>
  mutate(element = "estimate", star = !is.na(p) & p < opts$p_star)

smooths <- bind_rows(
  est |> filter(panel == "A") |> smooth_by("age", "y", group = c("panel", "sex", "group")),
  est |> filter(panel == "B") |> smooth_by("age", "y", group = c("panel", "sex", "group"), k = 5)
)

src <- bind_rows(est, smooths) |>
  mutate(y_variable = if_else(panel == "A", "Pearson r, height vs %BF", "Mean %BF"), .after = panel)
src <- write_source_data(src, 5)

pts <- function(id) filter(src, panel == .env$id, element == "estimate")
crv <- function(id) filter(src, panel == .env$id, element == "smooth")

## Figure 5A ###################################################################
pA <- ggplot() +
  geom_hline(yintercept = 0, linetype = "dashed", linewidth = 0.8) +
  geom_vline(xintercept = 11.5, linetype = "dotted", linewidth = 0.8) +
  geom_line(data = crv("A"), aes(x, y, colour = sex), linewidth = 1.1) +
  geom_errorbar(data = pts("A"), aes(x, ymin = lo, ymax = hi, colour = sex), width = 0.2, linewidth = 0.6) +
  geom_point(data = pts("A"), aes(x, y, colour = sex), size = 3) +
  geom_point(data = pts("A") |> filter(star), aes(x, if_else(y > 0, hi + 0.025, lo - 0.025), colour = sex),
             shape = 8, size = 1.8, show.legend = FALSE) +
  scale_colour_manual(values = pal_sex, name = "Sex") +
  scale_x_continuous(breaks = 8:20) +
  scale_y_continuous(labels = lab_minus) +
  labs(x = "Age (years)", y = "Pearson's r between height and %BF") +
  theme_bmikids() +
  theme(panel.grid.minor.x = element_blank())

## Figure 5B ###################################################################
q_levels <- names(q_off)
q_cols   <- set_names(RColorBrewer::brewer.pal(8, "Greys")[c(3, 4, 6, 8)], q_levels)

pB <- ggplot() +
  geom_linerange(data = pts("B"), aes(x, ymin = lo, ymax = hi, colour = group), alpha = 0.6, show.legend = FALSE) +
  geom_line(data = crv("B"), aes(x, y, colour = group), linewidth = 0.7) +
  geom_point(data = pts("B"), aes(x, y, colour = group, shape = group), size = 2.6) +
  facet_wrap(~ sex) +
  scale_colour_manual(values = q_cols, limits = q_levels, name = "Height quartile") +
  scale_shape_manual(values = set_names(c(16, 17, 18, 15), q_levels), limits = q_levels, name = "Height quartile") +
  scale_x_continuous(breaks = seq(8, 20, 2)) +
  labs(x = "Age (years)", y = "Mean percent body fat") +
  theme_bmikids()

fig5 <- (pA / pB) + plot_annotation(tag_levels = "A")
save_figure(fig5, "Figure5", width = 8, height = 10)

sessioninfo::session_info()
