# File Metadata ################################################################
# Title: Graphical abstract — "Same BMI, different body"
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 23 Nov 2026
# Version: 3.0
#
# Input : data/derived/res_youth_curves.rds (03_kids.Rmd; the same %BF estimates as Figure 8B)
#         data/derived/kids.rds              (01_data_wrangling.R; median heights)
# Output: results/source_data/SourceData_GraphicalAbstract.csv
#         results/figures/graphical_abstract.{eps,tiff,pdf,png}
#
# Obesity Reviews: TIFF or EPS, within 50 mm x 60 mm, >= 300 dpi, no fine detail.
# The canvas is drawn in millimetres (1 data unit = 1 mm) on a 50 x 60 mm page.
# Two boys with BMI 25 kg/m² at ages 8 and 20, drawn at their median heights. Each body is
# shaded so that the share of its area above the dividing line equals its %BF (fat, dark)
# and the rest its lean share (light). Shading shows proportions, not where the fat is.
# Shapes are built with polyclip (offsets, unions and clipping of polygons).

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

## settings ####################################################################
page_w  <- 50; page_h <- 60                       # mm
ages    <- c(8, 20)
col_ga  <- c(fat = "#595959", lean = "#CFCFCF", ink = "#1A1A1A", sub = "#555555",
             fat_lab = "#333333", lean_lab = "#7A7A7A", cross = "#C0392B")
pt      <- function(x) x / .pt                    # font size in points -> ggplot text size
lw_pt   <- function(x) x * 96 / 72 / .pt          # line width in points -> ggplot linewidth

## source data #################################################################
heights <- read_result("kids") |>
  filter(sex == "Males", age %in% ages) |>
  summarise(median_height_cm = median(height_cm), n_height = n(), .by = age)

ga_data <- read_result("res_youth_curves")$pbf25 |>
  filter(sex == "Males", as.character(age_grp) %in% ages) |>
  transmute(sex = as.character(sex), age = as.numeric(as.character(age_grp)), bmi = ref_bmi,
            pbf, lo, hi, n, lean_pct = 100 - pbf,
            fmi = bmi * pbf / 100, ffmi = bmi * (100 - pbf) / 100) |>
  left_join(heights, by = "age")

f <- fs::path(paths$out_source, "SourceData_GraphicalAbstract.csv")
write_excel_csv(ga_data, f, na = "")
ga <- read_csv(f, show_col_types = FALSE)                 # draw from the file, as for every figure
cli::cli_alert_success("Wrote {.file {f}}")

## polygon geometry ############################################################
# A shape is a list of rings, each list(x, y), as used by polyclip.

#' Circle of radius r
circle <- function(x, y, r, n = 120) {
  t <- seq(0, 2 * pi, length.out = n + 1)[-1]
  list(list(x = x + r * cos(t), y = y + r * sin(t)))
}

#' A polyline thickened to radius r with round ends (a limb)
cap <- function(pts, r) {
  polyclip::polylineoffset(list(x = pts[, 1], y = pts[, 2]), r, jointype = "round", endtype = "openround")
}

#' A polygon grown by r with rounded corners
grow <- function(pts, r) polyclip::polyoffset(list(x = pts[, 1], y = pts[, 2]), r, jointype = "round")

union_all <- function(parts) Reduce(\(a, b) polyclip::polyclip(a, b, op = "union"), parts)

#' Area of a shape (holes have the opposite orientation, so the signed sum is the net area)
shape_area <- function(s) abs(sum(map_dbl(s, \(p) sum(p$x * c(p$y[-1], p$y[1]) - c(p$x[-1], p$x[1]) * p$y) / 2)))

bbox <- function(s) c(xmin = min(map_dbl(s, \(p) min(p$x))), xmax = max(map_dbl(s, \(p) max(p$x))),
                      ymin = min(map_dbl(s, \(p) min(p$y))), ymax = max(map_dbl(s, \(p) max(p$y))))

#' Part of a shape between heights y0 and y1
slice <- function(s, y0, y1) {
  b <- bbox(s)
  rect <- list(x = c(b[["xmin"]] - 1, b[["xmax"]] + 1, b[["xmax"]] + 1, b[["xmin"]] - 1), y = c(y0, y0, y1, y1))
  polyclip::polyclip(s, rect, op = "intersect")
}

#' Height y at which the share of the shape's area above y equals frac_top
area_split <- function(s, frac_top) {
  b <- bbox(s); total <- shape_area(s)
  uniroot(\(y) shape_area(slice(s, y, b[["ymax"]] + 1)) / total - frac_top,
          c(b[["ymin"]], b[["ymax"]]), tol = 1e-6)$root
}

#' Scale a unit-height shape to `height` mm and put its feet at (cx, y0)
place <- function(s, cx, y0, height) map(s, \(p) list(x = cx + p$x * height, y = y0 + p$y * height))

#' Shape -> data frame for geom_polygon (one subgroup per ring, so holes stay holes)
shape_df <- function(s) imap_dfr(s, \(p, i) tibble(x = p$x, y = p$y, ring = i))

## silhouettes (unit height: feet at y = 0, top of head at y = 1) ##############
m <- function(...) matrix(c(...), ncol = 2, byrow = TRUE)

adult <- function() union_all(c(
  list(circle(0, 0.930, 0.062)),
  list(cap(m(0, 0.87, 0, 0.83), 0.026)),
  list(grow(m(-0.040, 0.835,  0.040, 0.835,  0.108, 0.800,  0.112, 0.770,  0.090, 0.640,
               0.074, 0.560,  0.085, 0.475, -0.085, 0.475, -0.074, 0.560, -0.090, 0.640,
              -0.112, 0.770, -0.108, 0.800), 0.012)),                                   # torso
  list(cap(m(-0.118, 0.785, -0.140, 0.640, -0.150, 0.490), 0.022),                      # arms
       cap(m( 0.118, 0.785,  0.140, 0.640,  0.150, 0.490), 0.022)),
  list(cap(m(-0.040, 0.475, -0.046, 0.260, -0.050, 0.035), 0.034),                      # legs
       cap(m( 0.040, 0.475,  0.046, 0.260,  0.050, 0.035), 0.034)),
  list(cap(m(-0.052, 0.018, -0.085, 0.018), 0.018),                                     # feet
       cap(m( 0.052, 0.018,  0.085, 0.018), 0.018))
))

child <- function() union_all(c(
  list(circle(0, 0.900, 0.096)),                                                        # larger head
  list(cap(m(0, 0.81, 0, 0.77), 0.032)),
  list(grow(m(-0.040, 0.775,  0.040, 0.775,  0.096, 0.748,  0.106, 0.705,  0.124, 0.610,
               0.124, 0.520,  0.106, 0.405, -0.106, 0.405, -0.124, 0.520, -0.124, 0.610,
              -0.106, 0.705, -0.096, 0.748), 0.016)),                                   # rounder belly
  list(cap(m(-0.124, 0.728, -0.152, 0.590, -0.160, 0.450), 0.027),
       cap(m( 0.124, 0.728,  0.152, 0.590,  0.160, 0.450), 0.027)),
  list(cap(m(-0.050, 0.405, -0.053, 0.220, -0.055, 0.040), 0.043),                      # shorter legs
       cap(m( 0.050, 0.405,  0.053, 0.220,  0.055, 0.040), 0.043)),
  list(cap(m(-0.057, 0.022, -0.090, 0.022), 0.022),
       cap(m( 0.057, 0.022,  0.090, 0.022), 0.022))
))

## one body: shape split into fat (top) and lean (bottom) ######################
#' @param dx Horizontal offset of the Fat/Lean labels from the body centre (mm).
body_layers <- function(shape, cx, base, height, pbf, age, dx) {
  g  <- place(shape, cx, base, height)
  b  <- bbox(g)
  ys <- area_split(g, pbf / 100)
  poly <- function(s, fill, ...) geom_polygon(data = shape_df(s), aes(x, y, subgroup = ring),
                                              fill = fill, colour = NA, ...)
  list(
    poly(slice(g, ys, b[["ymax"]] + 1), col_ga[["fat"]]),
    poly(slice(g, base - 1, ys), col_ga[["lean"]]),
    poly(slice(g, ys - 0.12, ys + 0.12), "white"),                                    # fat/lean boundary
    geom_polygon(data = shape_df(g), aes(x, y, subgroup = ring), fill = NA,
                 colour = col_ga[["ink"]], linewidth = lw_pt(0.5), linejoin = "round"),
    annotate("text", x = cx + dx, y = (ys + b[["ymax"]]) / 2, label = sprintf("Fat\n%.0f%%", pbf),
             size = pt(5.6), fontface = "bold", lineheight = 0.85, colour = col_ga[["fat_lab"]]),
    annotate("text", x = cx + dx, y = (base + ys) / 2, label = sprintf("Lean\n%.0f%%", 100 - pbf),
             size = pt(5.6), fontface = "bold", lineheight = 0.85, colour = col_ga[["lean_lab"]]),
    annotate("text", x = cx, y = base - 3.3, label = paste("Age", age),
             size = pt(7), fontface = "bold", colour = col_ga[["ink"]])
  )
}

#' Red cross mark (two lines; no special font needed)
cross <- function(x, y, s = 0.8) {
  annotate("segment", x = c(x - s, x - s), y = c(y - s, y + s), xend = c(x + s, x + s), yend = c(y + s, y - s),
           colour = col_ga[["cross"]], linewidth = lw_pt(1.1), lineend = "round")
}

## the graphical abstract ######################################################
base     <- 6.5                                   # feet (mm)
h_adult  <- 40                                    # adult stature on the page (mm)
h_ratio  <- ga$median_height_cm[ga$age == 8] / ga$median_height_cm[ga$age == 20]
pbf_at   <- \(a) ga$pbf[ga$age == a]
failures <- tibble(y = 44.2 - 0:2 * 2.75,
                   label = c("<b>scaling</b>", "<b>height–fat <i>r</i></b>", "<b>composition</b>"))

p <- ggplot() +
  annotate("text", x = page_w / 2, y = 56.6, label = "Same BMI, different body",
           size = pt(8.5), fontface = "bold", colour = col_ga[["ink"]]) +
  annotate("text", x = page_w / 2, y = 53.0, label = "Boys with BMI 25 kg/m²",
           size = pt(6.3), fontface = "bold", colour = col_ga[["sub"]]) +
  body_layers(child(), 11.5, base, h_adult * h_ratio, pbf_at(8),  8,  dx = 7.8) +
  body_layers(adult(), 35.0, base, h_adult,           pbf_at(20), 20, dx = 10.2) +
  ## the three adult assumptions that fail during growth, above the child
  annotate("text", x = 1.2, y = 50.4, label = "Adult BMI assumptions\nfail in children:",
           hjust = 0, vjust = 1, size = pt(5.6), fontface = "bold", lineheight = 0.95,
           colour = col_ga[["ink"]]) +
  map(failures$y, \(y) cross(2.0, y)) +
  ggtext::geom_richtext(data = failures, aes(x = 3.6, y = y, label = label), hjust = 0,
                        size = pt(5.6), colour = col_ga[["ink"]],
                        fill = NA, label.colour = NA, label.padding = unit(0, "pt")) +
  coord_fixed(xlim = c(0, page_w), ylim = c(0, page_h), expand = FALSE) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", colour = NA))

## save ########################################################################
mm <- 1 / 25.4
save_figure(p, "graphical_abstract", page_w * mm, page_h * mm)

sessioninfo::session_info()
