create table countries(
    country_id SERIAL NOT NULL,
    country_name VARCHAR(40) NOT NULL,
    PRIMARY KEY ( country_id )
);



INSERT INTO countries (country_name) VALUES ('Germany');

INSERT INTO countries (country_name) VALUES ('Poland');

INSERT INTO countries (country_name) VALUES ('United Kingdom');

INSERT INTO countries (country_name) VALUES ('Italy');

INSERT INTO countries (country_name) VALUES ('Portugal');