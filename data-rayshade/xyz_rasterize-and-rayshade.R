# nohup R < data-rayshade/xyz_rasterize-and-rayshade.R --vanilla > data-rayshade/xyz_rasterize-and-rayshade_2023-12-18.log &

# Here we rasterize data from each of 1e5 x 1e5 meter squares, square number being
#  a variable in the parquet-data. The square model is described in model.qmd
#  One could have used x-y conditions, but this is more convenient programatically
#  The reaons for splitting is mostly to not overfry the comptuter
#
# The little-helper function (mb_rashade_xyz_dynamic) used below could
#  become part of {ramb} but then it needs to be more generalized. It is
#  kind of a dynamic rasterization, to a degree dependent on the number of
#  pings available in the raster being squared.
#
# INPUT: data-parquet/xyz
#   The data used is generated via the data-parquet/DATASET_xyz.R
# OUTPUT: for now: data-rayshade/xyz/square_xx.tif
#  These are rayshaded rasters to be then used for png-tiling
#
# TO CHECK
#  Skip rayshading if all values are NA at a specific resolution. Need
#   then also to take into account the "cover" part, because file
#   numbers will be fewer. E.g. if all NA at the highest resolution
#   (8 x 8 meters) then there would be no temporary raster rgb file.

library(fs)
library(terra)
library(tidyterra)
library(tidyverse)
library(arrow)
source("R/mb_base_raster.R")
source("R/mb_rayshade_xyz_dynamic.R")

CRS <- 5325
con <- open_dataset("data-parquet/xyz")
con |> glimpse()
con |> dplyr::filter(is.na(sq)) |> collect()
r0 <-
  mb_base_raster() |>
  terra::aggregate(1024) |>
  terra::disagg(1024)
r0 <- r0 |> aggregate(2) # 8 meter raster the highest resolution
SQ <- c(10:13, 16:20, 23:26, 30:31) # check-out model.qmd
# SQ <- 25 # Vestmannaeyjar as a test
# resume
SQ <- c(17:20, 23:26, 30:31) # check-out model.qmd
for(s in 1:length(SQ)) {
  print(paste0("doing ", s, " of ", length(SQ), " (tile ", SQ[s], ") squares ------------------------"))
  xyz <-
    con |>
    dplyr::filter(sq %in% SQ[s]) |>
    collect()
  if(nrow(xyz) > 1000) {
    rs <-
      xyz |>
      mb_rashade_xyz_dynamic(r0, file_prefix = paste0("tmp/sq", SQ[s]), ping_cutoff = 10, max_meters = 128)
    # values(rs) <- as.integer(values(rs))
    rs |>
      writeRaster(paste0("data-rayshade/xyz/xyz_square_",
                         str_pad(SQ[s], width = 2, pad = "0"),
                         ".tif"),
                  overwrite = TRUE)
                  # inteter values, 8-bit, positive only
                  # datatype = "INT8U")
  }
}
