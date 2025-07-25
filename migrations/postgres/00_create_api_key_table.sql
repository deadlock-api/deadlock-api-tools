create table api_keys
(
 key            uuid      default gen_random_uuid() not null primary key,
 data_access    boolean   default false             not null,
 disabled       boolean   default false,
 esports_ingest boolean   default false,
 comment        text,
 user_id        uuid      default gen_random_uuid() not null,
 created_at     timestamp default current_timestamp
);

create table api_key_limits
(
 key         uuid     not null,
 path        text     not null,
 rate_limit  integer  not null,
 rate_period interval not null,
 created_at  timestamp default current_timestamp,
 primary key (key, path),
 foreign key (key) references api_keys (key)
);

create table api_key_account_groups
(
 key                uuid         not null,
 account_group_name varchar(255) not null,
 created_at         timestamp default current_timestamp,
 primary key (key, account_group_name),
 foreign key (key) references api_keys (key)
);
