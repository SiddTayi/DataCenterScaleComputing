-- Create the dimension tables first

-- Dimension tables
CREATE TABLE dim_animal (
    animal_id varchar PRIMARY KEY,
    name varchar,
    date_of_birth date,
    animal_type varchar,
    color varchar,
    kind varchar,
    sex varchar
);

CREATE TABLE dim_date (
    date_id serial PRIMARY KEY, 
    date date,
    time time,
    --datetime timestamp,
    --monthyear varchar
    month varchar,
    year int
);

CREATE TABLE dim_breed (
    breed_id serial PRIMARY KEY,
    breed varchar
    
);


CREATE TABLE dim_outcome(
    outcome_id serial PRIMARY KEY,
    outcome_type varchar,
    outcome_subtype varchar

);

-- Fact table
CREATE TABLE fact_animal (
    fact_id serial PRIMARY KEY,
    outcome_id int,
    animal_id varchar,
    date_id integer,
    breed_id integer,
    FOREIGN KEY (animal_id) REFERENCES dim_animal(animal_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (breed_id) REFERENCES dim_breed(breed_id),
     FOREIGN KEY (outcome_id) REFERENCES dim_outcome(outcome_id)
);
