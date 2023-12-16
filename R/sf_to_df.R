sf_to_df <- function(s) {

  bind_cols(s |> st_drop_geometry(),
            st_coordinates(s) |>
              as_tibble() |>
              rename(x = 1, y = 2))

}
