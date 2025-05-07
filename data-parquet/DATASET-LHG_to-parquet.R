# ------------------------------------------------------------------------------
# run this as:
#  nohup R < data-parquet/DATASET_to-parquet.R --vanilla > data-parquet/DATASET_to-parquet_2023-12-15.log &
# system("rm ~/stasi/gis/botninn/data-parquet/xyz/*.parquet")

# 2024-09-04: Added a code for tmp_david/arnarfj_Ext2.txt
# 2023-12-08 Added Julians data source
#  original: /net/hafkaldi.hafro.is/export/u3/haf/Julian/bathymetry/LHG_single_beam.csv
#  copy    : data-copy/julian/LHG_single_beam.csv
# 2023-12-06 No changes


print(lubridate::now())
# TO DO:
#  * Sandvík seem to have an x outlier


library(conflicted)
library(arrow)
library(sf)
library(tidyverse)
library(tictoc)
tic()
source("R/sf_to_df.R")
sq <- read_sf("data/mb_squares.gpkg") |> mutate(sq = as.integer(sq))
CRS <- 5325
# functions --------------------------------------------------------------------
mb_read_xyz <- function(pth, sep = " ") {

  org <- pth
  #if(stringr::str_ends(pth, ".gz")) {
  #  pth <- gzfile(pth)
  #}

  d <-
    read.table(pth, header = FALSE) |>
    tibble::as_tibble() |>
    dplyr::rename(x = 1, y = 2, z = 3) |>
    dplyr::mutate(z = -z,
                  file = basename(org),
                  pth = org)

  #if(stringr::str_ends(pth, ".gz")) {
  #  unlink(pth)
  #}

  return(d)

}

# 2024-09-04 trial file from david ---------------------------------------------
d <-
  arrow::read_csv_arrow("data-copy/tmp_david/arnarfj_Ext2.txt", col_names = FALSE) |>
  dplyr::filter(f12 == "Accepted") |>
  select(x = f0,
         y = f1,
         z = f2) |>
  st_as_sf(coords = c("x", "y"),
           crs = CRS,
           remove = FALSE) |>
  st_join(sq) |>
  st_drop_geometry() |>
  mutate(z = -z,
         area = "Arnarfjördur",
         source = "haf",
         file = "arnarfj_Ext2.txt",
         pth = "data-copy/tmp_david/arnarfj_Ext2.txt") |>
  select(x, y, z, sq, file, area, pth, source)
d |>
  arrow::write_parquet("data-parquet/xyz/arnarfj_Ext2.parquet")


# copy the mb LHG directory ----------------------------------------------------
# system("cp -rp /u5/mb/LHG data-copy/.")

fs::dir_tree("data-copy/LHG/", recurse = 0)

# Eyjafjörður ------------------------------------------------------------------
fs::dir_tree("data-copy/LHG/Eyjafj_Multibeam_LHG", recurse = 0)

pth <-
  "data-copy/LHG/Eyjafj_Multibeam_LHG/Eyjafj_MultiBeam_5m.xyz"
readLines(pth, n = 2)
d1 <-
  mb_read_xyz(pth)

pth <-
  "data-copy/LHG/Eyjafj_Multibeam_LHG/Eyjafj_SingleBeam_baldur_1994.txt"
readLines(pth, n = 2)
d2 <- mb_read_xyz(pth)

bind_rows(d1 |> sample_n(1e3),
          d2) |>
  st_as_sf(coords = c("x", "y"),
           crs = 3057) |>
  st_transform(crs = CRS) |>
  ggplot() +
  geom_sf(aes(colour = file), size = 0.01)

bind_rows(d1, d2) |>
  st_as_sf(coords = c("x", "y"),
           crs = 3057) |>
  st_transform(crs = CRS) |>
  st_join(sq) |>
  sf_to_df() |>
  mutate(area = "Eyjafjörður") |>
  select(x, y, z, sq, file, area, pth) |>
  mutate(source = "lhg") |>
  arrow::write_parquet("data-parquet/xyz/Eyjafjörður.parquet")

# LHG nýtt ---------------------------------------------------------------------
# unknown dataformat
#  possibly: https://github.com/pktrigg/pyxtf
#            https://pypi.org/project/pyxtf/
#            https://github.com/oysstu/pyxtf
#            https://www.huxlabs.com/2016/02/25/reading-an-xtf-file-in-python/
fs::dir_tree("data-copy/LHG/LHG nýtt/", recurse = 1)
# xtf trials - works on own computer, data is a mess though - unprocessed
# pip3 install pyxtf
# code examples based on: https://github.com/oysstu/pyxtf/blob/master/examples/sonar_example.ipynb
# import pyxtf
# input_file = r'lhg_test/XTF_K429_34_JD168/0001 - K429_34 - 0001.xtf'
# (fh, p) = pyxtf.xtf_read(input_file)
# print(fh)
#
# # The ChanInfo field is an array of XTFChanInfo objects
# # Note that the ChanInfo field always has a size of 6, even if the number of channels is less.
# # Use the fh.NumXChannels fields to calculate the number (the function xtf_channel_count does this)
# n_channels = fh.channel_count(verbose=True)
# actual_chan_info = [fh.ChanInfo[i] for i in range(0, n_channels)]
# print('Number of data channels: {}\n'.format(n_channels))
#
# # Print the first channel
# print(actual_chan_info[0])
#
# # Print the keys in the packets-dictionary
# print([key for key in p])


# database connection trials
if(FALSE) {
  library(dplyr)
  db <- "data-copy/LHG/LHG nýtt/DB_K429_34/0001 - K429_34 - 0001.db"
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = db)
  # may need to start a server
  con <- DBI::dbConnect(RMySQL::MySQL(), dbname = db)
  # for postgres need to start postgres server
}

# LHG_2003BaldurDATA -----------------------------------------------------------
fs::dir_tree("data-copy/LHG/LHG-2003BaldurDATA", recurse = 1)
pth <- dir("data-copy/LHG/LHG-2003BaldurDATA", full.names = TRUE, pattern = ".gz")
area <-
  tibble(file = basename(pth) |> sort()) |>
  mutate(area = c("Flatey", "Flóinn", "Gjögurtá", "Lundey"))
for(i in 1:length(pth)) {
  print(paste0(i, " of ", length(pth), " ", pth[i]))
  tmp <- basename(pth[i]) |> tools::file_path_sans_ext()
  d <-
    mb_read_xyz(pth[i]) |>
    st_as_sf(coords = c("x", "y"),
             crs = 4326) |>
    st_transform(crs = CRS) |>
    st_join(sq) |>
    sf_to_df() |>
    mutate(area = area$area[i],
           z = -z) |>
    select(x, y, z, sq, file, area, pth) |>
    mutate(source = "lhg") |>
    arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", ".parquet"))
}

# LHG-Breidafjordur ------------------------------------------------------------
fs::dir_tree("data-copy/LHG/LHG-Breidafjordur", recurse = 1)
pth <- "data-copy/LHG/LHG-Breidafjordur/k426_dypi_allt.txt"
readLines(pth, n = 2)
# if(FALSE) {
convert_ddmmss <- function(x) {
  tibble(x) |>
    separate(x, c("dd", "mm", "ss"), sep = "-", remove = FALSE) |>
    # seonds to decimal minutes
    mutate(dm = as.numeric(ss) / 60 * 100,
           #mm = paste0(dd, mm, ss),
           x2 = paste0(dd, mm,
                       ifelse(dm < 10,
                              paste0("0", dm),
                              as.character(dm)))) |>
    mutate(x2 = geo::geoconvert.1(as.numeric(x2))) |>
    pull(x2)
}
d <-
  rio::import(pth, setclass = "tibble", fill = TRUE) |>
  select(lon = 2, lat = 1, z = 3) |>
  mutate(lon = str_remove(lon, "W"),
         lon = -convert_ddmmss(lon),
         lat = str_remove(lat, "N"),
         lat =  convert_ddmmss(lat)) |>
  drop_na()
if(FALSE) {
  d |>
    sample_n(1e4) |>
    ggplot(aes(lon, lat)) +
    geom_point(size = 0.01) +
    geom_path(data = geo::bisland) +
    coord_quickmap(xlim = range(d$lon), ylim = range(d$lat))
}
d |>
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |>
  st_transform(crs = CRS) |>
  st_join(sq) |>
  sf_to_df() |>
  mutate(pth = pth,
         file = basename(pth),
         area = "Breiðafjörður") |>
  select(x, y, z, sq, file, area, pth) |>
  mutate(source = "lhg") |>
  arrow::write_parquet(paste0("data-parquet/xyz/", "Breiðafjörður", "__", "k426_dypi_allt", ".parquet"))

# LHG-Vestmanneyjar ------------------------------------------------------------
fs::dir_tree("data-copy/LHG/LHG-Vestmanneyjar", recurse = 1)
# CRS 32627
pth <- dir("data-copy/LHG/LHG-Vestmanneyjar", full.names = TRUE, pattern = "txt")
readLines(pth[1], n = 2)
area <-
  tibble(file = basename(pth) |> sort()) |>
  mutate(area = c("Vestmannaeyjar"))
for(i in 1:length(pth)) {
  print(paste0(i, " of ", length(pth), " ", pth[i]))
  tmp <- basename(pth[i]) |> tools::file_path_sans_ext()
  rio::import(pth[i], setclass = "tibble", fill = TRUE) |>
    rename(x = 1,
           y = 2,
           z = 3) |>
    mutate(z = -z,
           file = basename(pth[i]),
           pth = pth[i]) |>
    st_as_sf(coords = c("x", "y"),
             crs = 32627) |>
    st_transform(crs = CRS) |>
    st_join(sq) |>
    sf_to_df() |>
    mutate(area = area$area[i]) |>
    select(x, y, z, sq, file, area, pth) |>
    mutate(source = "lhg") |>
    arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", tmp, ".parquet"))
}

# LHG_Austf --------------------------------------------------------------------
fs::dir_tree("data-copy/LHG/LHG_Austf/", recurse = 1)
pth <-
  dir("data-copy/LHG/LHG_Austf", pattern = ".txt", recursive = TRUE,
      full.names = TRUE)
area <-
  c(rep("Breiðdalsvík", 1),
    rep("Fáskrúðsfjörður", 3),
    rep("Loðmundarfjörður", 1),
    rep("Mjóifjörður", 3),
    rep("Norðfjörður", 3),
    rep("Reyðarfjörður", 6),
    rep("Sandvík", 1),
    rep("Seyðisfjörður", 3),
    rep("Stöðvarfjörður", 2),
    rep("Vaðlavík", 1),
    rep("Víkur", 2))
area <- c(area, area)
area <-
  tibble(file = basename(pth)) |>
  mutate(area = area)
for(i in 1:length(pth)) {
  print(paste0(i, " of ", length(pth), " ", pth[i]))
  tmp <- basename(pth[i]) |> tools::file_path_sans_ext()
  d <-
    rio::import(pth[i], setclass = "tibble", fill = TRUE) |>
    rename(x = 2,
           y = 1,
           z = 3) |>
    mutate(z = -z) |>
    dplyr::filter(!is.na(x), !is.na(y)) |>
    st_as_sf(coords = c("x", "y"),
             crs = 4326) |>
    st_transform(crs = CRS) |>
    st_join(sq) |>
    sf_to_df() |>
    mutate(file = basename(pth[i]),
           pth = pth[i],
           area = area$area[i]) |>
    select(x, y, z, sq, file, area, pth) |>
    mutate(source = "lhg")
  if(area$area[i] == "Sandvík") {
    d <- d |> dplyr::filter(x > 1.9e6)
  }
  d |>
    arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", tmp, ".parquet"))
}

# LHG_BALDUR_GOGN_SBP_MB -------------------------------------------------------
fs::dir_tree("data-copy/LHG/LHG_BALDUR_GOGN_SBP_MB", recurse = 1)

# LHG_Breidafjordur_juli2019 ---------------------------------------------------
fs::dir_tree("data-copy/LHG/LHG_Breidafjordur_juli2019", recurse = 1)
# ergo: nothing of use here

# LHG_Kolgrafarfjordur ---------------------------------------------------------
# CRS: plain lon-lat
fs::dir_tree("data-copy/LHG/LHG_Kolgrafarfjordur", recurse = 1)
#  directory Bin1 seems to hold higher resolution data
pth <- dir("data-copy/LHG/LHG_Kolgrafarfjordur", full.names = TRUE)
readLines(pth[1], n = 2)
area <-
  tibble(file = basename(pth) |> sort()) |>
  mutate(area = c("Kolgrafarfjörður"))
for(i in 1:length(pth)) {
  print(paste0(i, " of ", length(pth), " ", pth[i]))
  tmp <- basename(pth[i]) |> tools::file_path_sans_ext()
  mb_read_xyz(pth[i]) |>
    st_as_sf(coords = c("x", "y"),
             crs = 32627) |>
    st_transform(crs = CRS) |>
    st_join(sq) |>
    sf_to_df() |>
    mutate(area = area$area[i]) |>
    select(x, y, z, sq, file, area, pth) |>
    mutate(source = "lhg") |>
    arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", tmp, ".parquet"))
}

# LHG_Vestf xyz ----------------------------------------------------------------
# CRS: plain lon-lat for patro, rest is crs = 32627
fs::dir_tree("data-copy/LHG/LHG_Vestf", recurse = 1)
pth <-
  fs::dir_ls("data-copy/LHG/LHG_Vestf/", recurse = TRUE, glob = "*.xyz") |>
  as.vector()
area <-
  tibble(file = basename(pth)) |>
  mutate(area = c(rep("Patreksfjörður", 4),
                  rep("Arnarfjördur", 2),
                  rep("Dýrafjörður", 3),
                  rep("Önundarfjörður", 2),
                  "Súgandafjörður"))
for(i in 1:length(pth)) {
  print(paste0(i, " of ", length(pth), " ", pth[i]))
  tmp <- basename(pth[i]) |> tools::file_path_sans_ext()
  print(readLines(pth[i], n = 2))
  d <-
    read.table(pth[i]) |>
    as_tibble() |>
    rename(x = 2,
           y = 1,
           z = 3) |>
    mutate(z = -z) |>
    dplyr::filter(!is.na(x), !is.na(y))

  if(str_starts(tmp, "patro")) {
    d <-
      d |>
      st_as_sf(coords = c("x", "y"),
               crs = 4326) |>
      st_transform(crs = CRS) |>
      st_join(sq)
  } else {
    d <-
      d |>
      rename(x = y,
             y = x) |>
      st_as_sf(coords = c("x", "y"),
               crs =  32627) |>
      st_transform(crs = CRS) |>
      st_join(sq)
  }


  st_coordinates(d) |> as_tibble() |> rename(x = 1, y = 2) |>
    mutate(z = d$z,
           sq = d$sq,
           pth = pth[i],
           area = area$area[i],
           file = basename(pth[i])) |>
    dplyr::select(x, y, z, sq, file, area, pth) |>
    mutate(source = "lhg") |>
    arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", tmp, "-xyz.parquet"))
}

# LHG_Vestf txt ----------------------------------------------------------------
#
fs::dir_tree("data-copy/LHG/LHG_Vestf", recurse = 1)
pth <-
  fs::dir_ls("data-copy/LHG/LHG_Vestf/", recurse = TRUE, glob = "*.txt") |>
  as.vector()
# only bin-data
pth <- pth[str_detect(pth, "bin")]
area <-
  tibble(file = basename(pth)) |>
  mutate(area = c(rep("Patreksfjörður", 4),
                  rep("Tálknafjörður", 3),
                  rep("Dýrafjörður", 3)))
for(i in 1:length(pth)) {
  print(paste0(i, " of ", length(pth), " ", pth[i]))
  tmp <- basename(pth[i]) |> tools::file_path_sans_ext()
  print(readLines(pth[i], n = 2))
  if(str_starts(tmp, "Patro")) {
    read.table(pth[i]) |>
      as_tibble() |>
      rename(x = 2,
             y = 1,
             z = 3) |>
      mutate(x = as.numeric(x),
             y = as.numeric(y)) |>
      mutate(z = -z) |>
      st_as_sf(coords = c("x", "y"),
               crs =  32627) |>
      st_transform(crs = CRS) |>
      st_join(sq) |>
      sf_to_df() |>
      mutate(pth = pth[i],
             area = area$area[i],
             file = basename(pth[i])) |>
      select(x, y, z, sq, file, area, pth) |>
      mutate(source = "lhg") |>
      arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", tmp, "-txt.parquet"))
  } else {
    read.table(pth[i]) |>
      as_tibble() |>
      rename(x = 2,
             y = 1,
             z = 3) |>
      mutate(x = as.numeric(x),
             y = as.numeric(y)) |>
      mutate(z = -z) |>
      st_as_sf(coords = c("x", "y"),
               crs =  4326) |>
      st_transform(crs = CRS) |>
      st_join(sq) |>
      sf_to_df() |>
      mutate(pth = pth[i],
             area = area$area[i],
             file = basename(pth[i])) |>
      select(x, y, z, sq, file, area, pth) |>
      mutate(source = "lhg") |>
      arrow::write_parquet(paste0("data-parquet/xyz/", area$area[i], "__", tmp, "-txt.parquet"))
  }
}

# LHG_Ísafjörður ---------------------------------------------------------------
# format: https://en.wikipedia.org/wiki/Generic_sensor_format
#         https://github.com/topics/bathymetry
fs::dir_tree("data-copy/LHG/LHG_Ísafjörður", recurse = 2)

# LHG Single beam (via Julian) ------------------------------------------------
pth <- "data-copy/julian/LHG_single_beam.csv"
CRS <- 5325
read_csv(pth) |>
  rename(z = depth) |>
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |>
  st_transform(crs = CRS) |>
  st_join(sq) |>
  sf_to_df() |>
  select(x, y, z, sq) |>
  mutate(file = basename(pth),
         area = "Ísland",
         pth = pth) |>
  mutate(source = "lhg") |>
  arrow::write_parquet("data-parquet/xyz/LHG_single_beam.parquet")

# MB XYZ Oracle data added to this folder (so now directory name a misnomer) -----
pth <- dir("data-copy/XYZ_Oracle_gagnagrunnur", recursive = TRUE, full.names = TRUE)
pth <- pth[-10] # skip Bauja_2004_byte8
pth <- pth[-16] # skip NA_fridur_byte9
areas <-
  tibble(pth = pth,
         fil = basename(pth)) |>
  arrange(fil) |>
  mutate(area = c(rep("Djúpáll", 3),
                  rep("Halinn", 5),
                  "Hryggur",
                  rep("Ísafjarðardjúp", 6),
                  rep("Kolluáll", 2),
                  "Langanes",                # NA-fridur
                  rep("Reykjaneshryggur", 3),
                  rep("Víkuráll", 4)))

cn <- c("lat", "lon", "depth", "date", "time", "project", "vessel", "line",
        "profile", "beam", "tide", "accuracy", "status", "amp_db", "amp_byte",
        "alng_angle", "arc_angle")

AREA <- areas$area |> unique()
# area
for(i in 1:length(AREA)) {
  print(AREA[i])
  fil <-
    areas |>
    dplyr::filter(area == AREA[i]) |>
    pull(pth)

  for(j in 1:length(fil)) {
    file <- basename(fil[j]) |> tools::file_path_sans_ext()
    print(file)
    xyz <-
      read.table(fil[j],
                 col.names = cn,
                 header = FALSE,
                 skip = 1) |>
      as_tibble() |>
      rename(x = lon, y = lat, z = depth) |>
      mutate(z = -abs(z)) |>
      st_as_sf(coords = c("x", "y"),
               crs = 4326) |>
      st_transform(crs = CRS) |>
      st_join(sq) |>
      sf_to_df()

    xyz <-
      xyz |>
      dplyr::filter(status == "A") |>
      mutate(area = AREA[i],
             file = basename(fil[j]),
             pth = fil[j])

    xyz |>
      select(x, y, z, sq, file, area, pth, everything()) |>
      arrow::write_parquet(paste0("data-parquet/XYZ_Oracle_gagnagrunnur/",
                                  AREA[i],
                                  "__",
                                  tools::file_path_sans_ext(basename(fil[j])),
                                  ".parquet"))
    xyz |>
      select(x, y, z, sq, file, area, pth) |>
      mutate(source = "haf") |>
      arrow::write_parquet(paste0("data-parquet/xyz/",
                                  AREA[i],
                                  "__",
                                  tools::file_path_sans_ext(basename(fil[j])),
                                  ".parquet"))
  }
}

toc()
devtools::session_info()