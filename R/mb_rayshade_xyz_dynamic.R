#' Dynamic rayshading
#'
#' @param xyz xxx
#' @param r0 xxx
#' @param ping_cutoff xxx
#' @param file_prefix xxx
#' @param max_meters Default 512
#'
#' @return
#' @export
#'
mb_rashade_xyz_dynamic <- function(xyz,
                                   r0,
                                   ping_cutoff = 4,
                                   file_prefix = "tmp/tmp",
                                   max_meters = 512

) {

  # overly complex
  r0m <- terra::res(r0)[1]
  agg = 2 ^ (1:10)  # 8, 16, 32, 64, 128, 256, 512, 1024 meters
  meters <- agg[agg >= r0m & agg <= max_meters]
  agg <- meters / min(meters)

  ## ensure extent
  # r0 <-
  #   r0 |>
  #   terra::aggregate(max(meters) / min(meters)) |>
  #   terra::disagg(max(meters) / min(meters))

  tfile <- paste0("tmp/r_", sample(1:1000, 1), ".tif")

  for(i in 1:length(agg)) {
    print(paste0("resolution ", i, " (", meters[i], " meters) of ", length(meters), " (", meters[length(meters)], " m)"))
    R <-
      xyz |>
      ramb::mb_rasterize_xyz(r0, fun = "mean", agg = agg[i])
    Rc <-
      xyz |>
      ramb::mb_rasterize_xyz(r0, fun = "count", agg = agg[i])
    # if fewer than X measures drop rayshading, use next level up
    #   done downstream
    v <- values(Rc)

    # should check if there are any values to rayshade or possibly some threshold
    RS <-
      R |>
      ramb::mb_rayshade_raster_rgb(zscale = meters / min(meters))

    if(i < max(res)) RS[v <= ping_cutoff] <- NA
    # some fix, something about "in memory" or not
    if(i == 1) {
      writeRaster(RS, tfile, overwrite = TRUE)
      RS <- rast(tfile)
    }
    if(i > 1) RS <- RS |> terra::disagg(agg[i])
    if(i == 1) e <- ext(RS)
    RS <- crop(RS, e)
    # check if this is needed and if this is efficient
    values(RS) <- values(RS) |> as.integer()
    terra::writeRaster(RS,
                       paste0(file_prefix, "_r", agg[i], ".tif"),
                       datatype = "INT8U",
                       overwrite = TRUE)
  }

  print("Doing cover")
  r <- rast(paste0(file_prefix, "_r", meters[1], ".tif"))
  for(i in 2:length(meters)) {
    r <-
      r |>
      cover(rast(paste0(file_prefix, "_r", meters[i], ".tif")))
  }

  return(r)

}