CREATE TABLE log_record (
	machine varchar,
	name varchar,
	log_record_timestamp varchar primary key,
	type varchar
);

CREATE TABLE task (
	machine varchar,
	name varchar,
	start_timestamp varchar,
	stop_timestamp varchar,
	primary key(machine, name)
);
