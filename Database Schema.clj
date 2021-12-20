Table dimDate as D {
  date_id int [pk]
  year int [not null]
  month int [not null]
  day int [not null]
  day_of_week int [not null]
  week_of_year int [not null]
}

Table dimCountry as C {
  country_id char[2] [pk]
  name varchar [not null]
  languages varchar[]
  gdp_per_capita double
}

Table dimAirport as A {
  airport_id char[4] [pk]
  name varchar [not null]
  municipality varchar [not null]
  state char[2]
}

Table factIngress as I {
  id int [pk, increment]
  date_id int [ref: > D.date_id]
  year int
  country_id char[2] [ref: > C.country_id]
  airport_id char[4] [ref: > A.airport_id]
  gender char
  age_bucket char
  temperature_bucket char
}
